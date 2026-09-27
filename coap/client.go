package coap

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	mrand "math/rand"
	"sync"
	"time"
)

// 客户端错误。
var (
	ErrTimeout       = errors.New("coap: request timeout after max retransmits")
	ErrReset         = errors.New("coap: peer reset the exchange")
	ErrStaleResponse = errors.New("coap: stale response for reused token")
	ErrTokenInUse    = errors.New("coap: token already in flight")
)

// ClientConfig 为客户端参数。
type ClientConfig struct {
	// AckTimeout 为首次重传基准超时（默认 2s）。
	AckTimeout time.Duration
	// AckRandomFactor 为超时随机化因子（默认 1.5）。
	AckRandomFactor float64
	// MaxRetransmit 为最大重传次数（默认 4）。
	MaxRetransmit int
	// MaxMessageSize 为单报文上限，超过则自动启用块传输（默认 1024）。
	MaxMessageSize int
	// Dedup 客户端侧去重配置（用于重复通知识别）。
	Dedup DedupConfig
}

func (c ClientConfig) withDefaults() ClientConfig {
	if c.AckTimeout <= 0 {
		c.AckTimeout = 2 * time.Second
	}
	if c.AckRandomFactor <= 0 {
		c.AckRandomFactor = 1.5
	}
	if c.MaxRetransmit < 0 {
		c.MaxRetransmit = 0
	}
	if c.MaxRetransmit == 0 {
		c.MaxRetransmit = 4
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = 1024
	}
	c.Dedup = c.Dedup.withDefaults()
	return c
}

// Response 为一次请求的最终结果。
type Response struct {
	Msg *Message
	// RTT 为首个响应到达耗时。
	RTT time.Duration
}

// pendingExchange 跟踪一个在途的可确认交换。
type pendingExchange struct {
	token    string
	peer     Addr
	reqMID   uint16
	ch       chan *Message // 响应投递（缓冲，避免阻塞读循环）
	emptyACK chan struct{} // 收到空 ACK（分离响应预告）
	reset    chan struct{} // 收到 RST
}

// Client 为 CoAP 客户端，支持并发在途请求与观察。
type Client struct {
	conn PacketConn
	cfg  ClientConfig

	midSeq uint32 // 消息编号计数器
	tokSeq uint64 // 令牌计数器

	mu      sync.Mutex
	pending map[string]*pendingExchange // key: token
	byMID   map[uint16]*pendingExchange // 空 ACK/RST 按 MID 匹配
	closed  bool
	closeCh chan struct{}

	dedup *Dedup // 入方向 CON（通知）去重

	rndMu sync.Mutex
	rnd   *mrand.Rand

	obsMu sync.Mutex
	obs   map[string]*observationState // key: token

	wg sync.WaitGroup
}

// NewClient 在指定连接上创建客户端并启动接收循环。
func NewClient(conn PacketConn, cfg ClientConfig) *Client {
	var seed [8]byte
	_, _ = rand.Read(seed[:])
	c := &Client{
		conn:    conn,
		cfg:     cfg.withDefaults(),
		midSeq:  uint32(binary.BigEndian.Uint16(seed[:2])),
		tokSeq:  binary.BigEndian.Uint64(seed[:]),
		pending: make(map[string]*pendingExchange),
		byMID:   make(map[uint16]*pendingExchange),
		closeCh: make(chan struct{}),
		dedup:   NewDedup(cfg.Dedup),
		rnd:     mrand.New(mrand.NewSource(int64(binary.BigEndian.Uint64(seed[:])))),
		obs:     make(map[string]*observationState),
	}
	c.wg.Add(1)
	go c.readLoop()
	return c
}

// nextMID 生成下一个消息编号。
func (c *Client) nextMID() uint16 {
	for {
		c.mu.Lock()
		c.midSeq++
		mid := uint16(c.midSeq)
		_, used := c.byMID[mid]
		c.mu.Unlock()
		if !used {
			return mid
		}
	}
}

// nextToken 生成全局唯一令牌（计数器+随机起点，生命周期内不复用）。
func (c *Client) nextToken() []byte {
	c.mu.Lock()
	c.tokSeq++
	seq := c.tokSeq
	c.mu.Unlock()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	return b[:]
}

// randomTimeout 计算首次重传超时：AckTimeout * [1, AckRandomFactor)。
func (c *Client) randomTimeout() time.Duration {
	c.rndMu.Lock()
	f := c.rnd.Float64()
	c.rndMu.Unlock()
	return time.Duration(float64(c.cfg.AckTimeout) * (1 + f*(c.cfg.AckRandomFactor-1)))
}

// Do 发送请求并等待响应；CON 请求自动指数退避重传，
// 超过 MaxRetransmit 仍未收到响应时返回 ErrTimeout，
// 且保证清理全部在途状态。
func (c *Client) Do(ctx context.Context, to Addr, req *Message) (*Response, error) {
	if req.Type != NON {
		req.Type = CON
	}
	if len(req.Token) == 0 {
		req.Token = c.nextToken()
	}
	// 超大负载走 Block1 分块上传。
	if len(req.Payload) > c.cfg.MaxMessageSize && req.IsRequest() {
		return c.doBlock1(ctx, to, req)
	}
	start := time.Now()
	resp, err := c.roundTrip(ctx, to, req)
	if err != nil {
		return nil, err
	}
	// 响应携带 Block2 且还有后续块时，自动拉取并重组。
	if resp != nil && resp.Code.IsResponse() {
		if b, ok := blockOption(resp.Options, OptionBlock2); ok && b.More {
			full, err := c.fetchBlock2(ctx, to, req, resp, b)
			if err != nil {
				return nil, err
			}
			resp = full
		}
	}
	return &Response{Msg: resp, RTT: time.Since(start)}, nil
}

// roundTrip 执行一次可确认交换：发送 CON，按指数退避重传，
// 直到收到匹配响应、被 RST、超时或上下文取消。
// 若收到空 ACK（分离响应），停止重传并继续等待响应。
func (c *Client) roundTrip(ctx context.Context, to Addr, msg *Message) (*Message, error) {
	if len(msg.Token) == 0 {
		msg.Token = c.nextToken()
	}
	msg.MessageID = c.nextMID()
	data, err := msg.Encode()
	if err != nil {
		return nil, err
	}
	if msg.Type == NON {
		// NON 无确认语义，发送即完成；响应（若有）由调用方另行处理。
		if err := c.conn.WriteTo(data, to); err != nil {
			return nil, err
		}
		return c.waitResponseOnly(ctx, to, msg)
	}

	p := &pendingExchange{
		token:    string(msg.Token),
		peer:     to,
		reqMID:   msg.MessageID,
		ch:       make(chan *Message, 4),
		emptyACK: make(chan struct{}, 1),
		reset:    make(chan struct{}, 1),
	}
	if !c.registerPending(p) {
		return nil, ErrTokenInUse
	}
	defer c.unregisterPending(p)

	if err := c.conn.WriteTo(data, to); err != nil {
		return nil, err
	}

	timeout := c.randomTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	retransmits := 0
	acked := false // 收到空 ACK 后停止重传

	for {
		select {
		case resp := <-p.ch:
			return resp, nil
		case <-p.reset:
			return nil, ErrReset
		case <-p.emptyACK:
			acked = true
			timer.Stop()
		case <-timer.C:
			if acked {
				continue
			}
			if retransmits >= c.cfg.MaxRetransmit {
				return nil, ErrTimeout
			}
			retransmits++
			if err := c.conn.WriteTo(data, to); err != nil {
				return nil, err
			}
			if timeout > math.MaxInt64/2 {
				timeout = math.MaxInt64
			} else {
				timeout *= 2
			}
			timer.Reset(timeout)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closeCh:
			return nil, ErrClosed
		}
	}
}

// waitResponseOnly 处理 NON 请求：不重传，仅短暂等待响应。
func (c *Client) waitResponseOnly(ctx context.Context, to Addr, msg *Message) (*Message, error) {
	p := &pendingExchange{
		token:    string(msg.Token),
		peer:     to,
		reqMID:   msg.MessageID,
		ch:       make(chan *Message, 4),
		emptyACK: make(chan struct{}, 1),
		reset:    make(chan struct{}, 1),
	}
	if !c.registerPending(p) {
		return nil, ErrTokenInUse
	}
	defer c.unregisterPending(p)
	select {
	case resp := <-p.ch:
		return resp, nil
	case <-p.reset:
		return nil, ErrReset
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeCh:
		return nil, ErrClosed
	}
}

func (c *Client) registerPending(p *pendingExchange) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if _, dup := c.pending[p.token]; dup {
		return false
	}
	c.pending[p.token] = p
	c.byMID[p.reqMID] = p
	return true
}

func (c *Client) unregisterPending(p *pendingExchange) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, p.token)
	delete(c.byMID, p.reqMID)
}

// readLoop 为客户端接收循环。
func (c *Client) readLoop() {
	defer c.wg.Done()
	for {
		data, from, err := c.conn.ReadFrom()
		if err != nil {
			c.failAll(ErrClosed)
			return
		}
		msg, err := Decode(data)
		if err != nil {
			continue // 畸形报文：丢弃，不影响状态机
		}
		c.dispatch(from, msg)
	}
}

// dispatch 按报文类型分发。
func (c *Client) dispatch(from Addr, msg *Message) {
	switch msg.Type {
	case CON:
		if msg.Code.IsResponse() {
			// 入方向 CON 去重：重复投递的通知/分离响应只处理一次，
			// 但仍需补发 ACK。
			if dup, _ := c.dedup.Check(from, msg.MessageID); dup {
				c.sendEmptyACK(from, msg.MessageID)
				return
			}
			c.dedup.Store(from, msg.MessageID, nil)
			c.sendEmptyACK(from, msg.MessageID)
			if c.handleNotification(from, msg) {
				return
			}
			c.deliverToPending(from, msg, false)
		}
		// 入方向 CON 请求：客户端不处理，直接忽略。
	case NON:
		if msg.Code.IsResponse() {
			if c.handleNotification(from, msg) {
				return
			}
			c.deliverToPending(from, msg, false)
		}
	case ACK:
		if msg.Code == CodeEmpty {
			c.signalEmptyACK(msg)
			return
		}
		if msg.Code.IsResponse() {
			c.deliverToPending(from, msg, true)
		}
	case RST:
		c.signalReset(msg)
	}
}

// deliverToPending 将响应投递给匹配的在途交换。
// matchMID 为 true（ACK 捎带响应）时要求消息编号与请求一致，
// 从而令牌被复用时，迟到的旧响应（旧 MID）不会被误配给新请求。
func (c *Client) deliverToPending(from Addr, msg *Message, matchMID bool) {
	c.mu.Lock()
	p := c.pending[string(msg.Token)]
	c.mu.Unlock()
	if p == nil || p.peer != from {
		return // 无匹配在途请求：迟到或重复的响应，静默丢弃
	}
	if matchMID && msg.MessageID != p.reqMID {
		return // 令牌复用场景下的陈旧响应
	}
	select {
	case p.ch <- msg:
	default:
	}
}

// signalEmptyACK 处理空 ACK：按 MID 找到交换，通知其停止重传。
func (c *Client) signalEmptyACK(msg *Message) {
	c.mu.Lock()
	p := c.byMID[msg.MessageID]
	c.mu.Unlock()
	if p == nil {
		return
	}
	select {
	case p.emptyACK <- struct{}{}:
	default:
	}
}

// signalReset 处理 RST：按 MID 找到交换并使其失败。
func (c *Client) signalReset(msg *Message) {
	c.mu.Lock()
	p := c.byMID[msg.MessageID]
	c.mu.Unlock()
	if p == nil {
		return
	}
	select {
	case p.reset <- struct{}{}:
	default:
	}
}

// sendEmptyACK 回复空 ACK。
func (c *Client) sendEmptyACK(to Addr, mid uint16) {
	ack := &Message{Type: ACK, Code: CodeEmpty, MessageID: mid}
	if data, err := ack.Encode(); err == nil {
		_ = c.conn.WriteTo(data, to)
	}
}

// failAll 使所有在途交换失败（连接关闭时调用）。
func (c *Client) failAll(err error) {
	c.mu.Lock()
	pendings := make([]*pendingExchange, 0, len(c.pending))
	for _, p := range c.pending {
		pendings = append(pendings, p)
	}
	c.mu.Unlock()
	for _, p := range pendings {
		select {
		case p.reset <- struct{}{}:
		default:
		}
	}
	c.obsMu.Lock()
	obs := make([]*observationState, 0, len(c.obs))
	for _, o := range c.obs {
		obs = append(obs, o)
	}
	c.obsMu.Unlock()
	for _, o := range obs {
		o.close()
	}
}

// Close 终止所有在途请求并释放资源。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	close(c.closeCh)
	err := c.conn.Close()
	c.wg.Wait()
	return err
}
