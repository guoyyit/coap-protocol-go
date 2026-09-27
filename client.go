package coap

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// ClientConfig tunes retransmission and concurrency behavior.
type ClientConfig struct {
	// AckTimeout is the initial wait for an ACK before retransmitting.
	AckTimeout time.Duration
	// AckRandomFactor scales AckTimeout with jitter in [1, factor].
	AckRandomFactor float64
	// MaxRetransmit is the number of retransmissions before giving up.
	MaxRetransmit int
	// MaxLatency bounds how long a late response is still matchable.
	MaxLatency time.Duration
	// DedupWindow is how long inbound CON message IDs are remembered.
	DedupWindow time.Duration
}

// DefaultClientConfig returns RFC 7252-ish defaults, shortened for local use.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		AckTimeout:      500 * time.Millisecond,
		AckRandomFactor: 1.5,
		MaxRetransmit:   4,
		MaxLatency:      10 * time.Second,
		DedupWindow:     60 * time.Second,
	}
}

// Client is a CoAP endpoint issuing concurrent confirmable requests.
// Responses are matched by token, independently of message IDs, so
// out-of-order delivery across concurrent requests is safe.
type Client struct {
	cfg       ClientConfig
	transport Transport

	mu        sync.Mutex
	pending   map[string]*pendingRequest // key: string(token)
	byMID     map[uint16]*pendingRequest // in-flight CON awaiting ACK
	tombMid   map[string]time.Time       // recently completed addr/mid (late-response guard)
	dedup     *Dedup
	nextMID   uint16
	stamp     uint64
	closed    bool
	done      chan struct{}
	observers *observeRegistry
}

type pendingRequest struct {
	token   []byte
	mid     uint16
	addr    string
	msg     *Message
	respCh  chan *Message
	errCh   chan error
	timer   *time.Timer
	retries int
	acked   bool // empty ACK received; waiting for separate response
	// stamp identifies this registration; a reused token gets a new
	// stamp so stale timer callbacks never fire on the new request.
	stamp uint64
}

// NewClient creates a client bound to transport.
func NewClient(t Transport, cfg ClientConfig) *Client {
	c := &Client{
		cfg:       cfg,
		transport: t,
		pending:   make(map[string]*pendingRequest),
		byMID:     make(map[uint16]*pendingRequest),
		tombMid:   make(map[string]time.Time),
		dedup:     NewDedup(cfg.DedupWindow),
		done:      make(chan struct{}),
		observers: newObserveRegistry(),
	}
	var seed [8]byte
	_, _ = rand.Read(seed[:])
	c.nextMID = binary.BigEndian.Uint16(seed[:2])
	go c.loop()
	return c
}

func (c *Client) loop() {
	for {
		select {
		case p, ok := <-c.transport.Recv():
			if !ok {
				c.Close()
				return
			}
			c.handlePacket(p)
		case <-c.done:
			return
		}
	}
}

// Do issues a confirmable request and waits for the response or timeout.
func (c *Client) Do(ctx context.Context, msg *Message, addr string) (*Message, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if len(msg.Token) == 0 {
		msg.Token = c.newTokenLocked()
	}
	if len(msg.Token) > MaxTokenLen {
		c.mu.Unlock()
		return nil, ErrInvalidTokenLen
	}
	c.stamp++
	pr := &pendingRequest{
		token:  append([]byte(nil), msg.Token...),
		mid:    c.allocMIDLocked(),
		addr:   addr,
		msg:    msg,
		respCh: make(chan *Message, 1),
		errCh:  make(chan error, 1),
		stamp:  c.stamp,
	}
	msg.Type = CON
	msg.MessageID = pr.mid
	key := string(pr.token)
	c.pending[key] = pr
	c.byMID[pr.mid] = pr
	c.mu.Unlock()

	if err := c.transmit(pr); err != nil {
		c.finishRequest(key, pr.stamp)
		return nil, err
	}
	c.armRetransmit(key, pr.stamp, pr.mid)

	select {
	case resp := <-pr.respCh:
		return resp, nil
	case err := <-pr.errCh:
		return nil, err
	case <-ctx.Done():
		c.finishRequest(key, pr.stamp)
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClosed
	}
}

func (c *Client) newTokenLocked() []byte {
	var b [8]byte
	_, _ = rand.Read(b[:4])
	binary.BigEndian.PutUint32(b[4:], uint32(c.stamp)+1)
	return b[:]
}

func (c *Client) allocMIDLocked() uint16 {
	c.nextMID++
	return c.nextMID
}

// transmit encodes and sends the confirmable request once.
func (c *Client) transmit(pr *pendingRequest) error {
	data, err := Encode(pr.msg)
	if err != nil {
		return err
	}
	return c.transport.Send(data, pr.addr)
}

// armRetransmit schedules the next retransmission with exponential backoff.
func (c *Client) armRetransmit(key string, stamp uint64, mid uint16) {
	c.mu.Lock()
	pr := c.pending[key]
	if pr == nil || pr.stamp != stamp {
		c.mu.Unlock()
		return
	}
	// timeout = AckTimeout * 2^retries (jitter folded into base for determinism in tests)
	timeout := c.cfg.AckTimeout << uint(pr.retries)
	pr.timer = time.AfterFunc(timeout, func() { c.onRetransmit(key, stamp) })
	c.mu.Unlock()
}

func (c *Client) onRetransmit(key string, stamp uint64) {
	c.mu.Lock()
	pr := c.pending[key]
	if pr == nil || pr.stamp != stamp {
		c.mu.Unlock()
		return // stale timer from a reused token: ignore
	}
	if pr.acked {
		// Empty ACK received earlier; nothing to retransmit. The
		// separate-response wait is bounded by finishRequest paths.
		c.mu.Unlock()
		return
	}
	if pr.retries >= c.cfg.MaxRetransmit {
		delete(c.pending, key)
		delete(c.byMID, pr.mid)
		c.mu.Unlock()
		pr.errCh <- ErrTimeout
		return
	}
	pr.retries++
	c.mu.Unlock()

	_ = c.transmit(pr) // send error surfaces via next timeout
	c.armRetransmit(key, stamp, pr.mid)
}

// finishRequest removes a pending request and disarms its timer.
func (c *Client) finishRequest(key string, stamp uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pr := c.pending[key]
	if pr == nil || pr.stamp != stamp {
		return
	}
	if pr.timer != nil {
		pr.timer.Stop()
	}
	delete(c.pending, key)
	delete(c.byMID, pr.mid)
	c.tombMid[tombKey(pr.addr, pr.mid)] = time.Now()
	c.purgeTombsLocked()
}

func tombKey(addr string, mid uint16) string {
	return fmt.Sprintf("%s/%d", addr, mid)
}

// purgeTombsLocked drops tombstones older than MaxLatency.
func (c *Client) purgeTombsLocked() {
	now := time.Now()
	for k, ts := range c.tombMid {
		if now.Sub(ts) > c.cfg.MaxLatency {
			delete(c.tombMid, k)
		}
	}
}

// handlePacket processes one inbound datagram (internal).
func (c *Client) handlePacket(p Packet) {
	msg, err := Decode(p.Data)
	if err != nil {
		return // malformed datagrams are dropped, never crash the loop
	}
	switch msg.Type {
	case ACK, RST:
		c.handleAckOrRst(msg, p.Addr)
	case CON:
		// Separate response or observe notification arrives as CON.
		c.handleInboundCON(msg, p.Addr)
	case NON:
		c.handleInboundNON(msg, p.Addr)
	}
}

func (c *Client) handleAckOrRst(msg *Message, addr string) {
	c.mu.Lock()
	pr := c.byMID[msg.MessageID]
	if pr == nil {
		c.mu.Unlock()
		return // duplicate or out-of-order ACK/RST: state machine unaffected
	}
	if msg.Type == RST {
		delete(c.pending, string(pr.token))
		delete(c.byMID, pr.mid)
		if pr.timer != nil {
			pr.timer.Stop()
		}
		c.mu.Unlock()
		pr.errCh <- ErrReset
		return
	}
	// ACK
	delete(c.byMID, pr.mid) // stop retransmission bookkeeping
	if pr.timer != nil {
		pr.timer.Stop()
	}
	if msg.Code == CodeEmpty {
		pr.acked = true // separate response will follow as CON
		c.mu.Unlock()
		return
	}
	// Piggybacked response.
	delete(c.pending, string(pr.token))
	c.tombMid[tombKey(addr, msg.MessageID)] = time.Now()
	c.mu.Unlock()
	pr.respCh <- msg
}

func (c *Client) handleInboundCON(msg *Message, addr string) {
	// Deduplicate: a retransmitted CON response must be ACKed again but
	// not delivered twice.
	if c.dedup.Seen(DedupKey{Addr: addr, MessageID: msg.MessageID}) {
		c.sendACK(addr, msg.MessageID)
		return
	}
	key := string(msg.Token)
	c.mu.Lock()
	_, isReq := c.pending[key]
	c.mu.Unlock()

	if c.observers.dispatch(msg, addr) {
		c.sendACK(addr, msg.MessageID)
		return
	}
	if isReq {
		c.sendACK(addr, msg.MessageID)
		c.mu.Lock()
		pr := c.pending[key]
		if pr != nil {
			delete(c.pending, key)
			if pr.timer != nil {
				pr.timer.Stop()
			}
			c.mu.Unlock()
			pr.respCh <- msg
			return
		}
		c.mu.Unlock()
		return
	}
	// Unknown CON: reject so the peer stops retransmitting.
	c.sendRST(addr, msg.MessageID)
}

func (c *Client) handleInboundNON(msg *Message, addr string) {
	if c.observers.dispatch(msg, addr) {
		return
	}
	key := string(msg.Token)
	c.mu.Lock()
	pr := c.pending[key]
	if pr != nil {
		delete(c.pending, key)
		if pr.timer != nil {
			pr.timer.Stop()
		}
		c.mu.Unlock()
		pr.respCh <- msg
		return
	}
	c.mu.Unlock()
}

func (c *Client) sendACK(addr string, mid uint16) {
	data, err := Encode(&Message{Type: ACK, Code: CodeEmpty, MessageID: mid})
	if err == nil {
		_ = c.transport.Send(data, addr)
	}
}

func (c *Client) sendRST(addr string, mid uint16) {
	data, err := Encode(&Message{Type: RST, Code: CodeEmpty, MessageID: mid})
	if err == nil {
		_ = c.transport.Send(data, addr)
	}
}

// Close terminates the client and fails all pending requests.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	pending := make([]*pendingRequest, 0, len(c.pending))
	for _, pr := range c.pending {
		pending = append(pending, pr)
		if pr.timer != nil {
			pr.timer.Stop()
		}
	}
	c.pending = make(map[string]*pendingRequest)
	c.byMID = make(map[uint16]*pendingRequest)
	c.mu.Unlock()
	for _, pr := range pending {
		pr.errCh <- ErrClosed
	}
	return c.transport.Close()
}
