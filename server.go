package coap

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Handler serves one request and returns the response message.
// Returning an error yields a 5.00 response.
type Handler func(req *Message, addr string) (*Message, error)

// ServerConfig tunes server behavior.
type ServerConfig struct {
	// DedupWindow is how long processed CON message IDs are remembered.
	DedupWindow time.Duration
	// ObserverTTL bounds how long a silent subscriber is kept.
	ObserverTTL time.Duration
}

// DefaultServerConfig returns sane local defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		DedupWindow: 60 * time.Second,
		ObserverTTL: 5 * time.Minute,
	}
}

// respCacheEntry caches a response to a processed CON so retransmitted
// requests get the same response without re-executing the handler.
type respCacheEntry struct {
	data []byte
	at   time.Time
}

// notifRef maps a notification message ID to its subscriber so a RST
// removes the dead subscriber.
type notifRef struct {
	addr  string
	token []byte
}

// Server is a CoAP endpoint serving resources and pushing observations.
type Server struct {
	cfg       ServerConfig
	transport Transport

	mu        sync.Mutex
	handlers  map[string]Handler
	dedup     *Dedup
	respCache map[DedupKey]respCacheEntry
	observers *observeStore
	notif     map[uint16]notifRef
	blocks    map[string]*blockReassembler // inbound Block1 reassembly, key addr|token
	nextMID   uint16
	closed    bool
	done      chan struct{}
	stateDir  string
}

// NewServer creates a server bound to transport. stateDir may be "" to
// disable persistence; otherwise dedup and observe state survive restarts.
func NewServer(t Transport, cfg ServerConfig, stateDir string) *Server {
	s := &Server{
		cfg:       cfg,
		transport: t,
		handlers:  make(map[string]Handler),
		dedup:     NewDedup(cfg.DedupWindow),
		respCache: make(map[DedupKey]respCacheEntry),
		observers: newObserveStore(),
		notif:     make(map[uint16]notifRef),
		blocks:    make(map[string]*blockReassembler),
		done:      make(chan struct{}),
		stateDir:  stateDir,
	}
	// Randomize the message ID start so a restart does not collide with
	// message IDs peers still remember in their dedup windows.
	var seed [2]byte
	_, _ = rand.Read(seed[:])
	s.nextMID = binary.BigEndian.Uint16(seed[:])
	if stateDir != "" {
		s.restore()
	}
	go s.loop()
	return s
}

// Handle registers a handler for a path (e.g. "/temp").
func (s *Server) Handle(path string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[path] = h
}

// NotifyChanged pushes a notification to all live observers of path.
func (s *Server) NotifyChanged(path string, payload []byte) {
	s.mu.Lock()
	subs := s.observers.forPath(path, s.cfg.ObserverTTL)
	s.mu.Unlock()
	for _, sub := range subs {
		s.mu.Lock()
		s.nextMID++
		mid := s.nextMID
		sub.Seq++
		seq := sub.Seq
		s.notif[mid] = notifRef{addr: sub.Addr, token: sub.Token}
		s.mu.Unlock()

		msg := &Message{
			Type:      CON,
			Code:      CodeContent,
			MessageID: mid,
			Token:     sub.Token,
			Payload:   payload,
		}
		msg.SetUint(OptionObserve, seq)
		data, err := Encode(msg)
		if err != nil {
			continue
		}
		_ = s.transport.Send(data, sub.Addr)
	}
	s.persist()
}

// Close stops the server and persists state.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	s.persist()
	return s.transport.Close()
}

func (s *Server) loop() {
	for {
		select {
		case p, ok := <-s.transport.Recv():
			if !ok {
				return
			}
			s.handlePacket(p)
		case <-s.done:
			return
		}
	}
}

// handlePacket processes one inbound datagram (internal).
func (s *Server) handlePacket(p Packet) {
	msg, err := Decode(p.Data)
	if err != nil {
		return // malformed: drop silently, never crash
	}
	switch msg.Type {
	case CON:
		// Handlers run concurrently so a slow resource never blocks
		// the receive loop; dedup keeps redeliveries idempotent.
		go s.handleRequest(msg, p.Addr, true)
	case NON:
		go s.handleRequest(msg, p.Addr, false)
	case ACK:
		// ACK of a notification: nothing to do (bookkeeping optional).
	case RST:
		s.mu.Lock()
		if ref, ok := s.notif[msg.MessageID]; ok {
			delete(s.notif, msg.MessageID)
			s.observers.remove(ref.addr, ref.token) // dead subscriber
			s.persistLocked()
		}
		s.mu.Unlock()
	}
}

func (s *Server) handleRequest(msg *Message, addr string, confirmable bool) {
	key := DedupKey{Addr: addr, MessageID: msg.MessageID}
	if confirmable {
		s.mu.Lock()
		dup := s.dedup.Seen(key)
		cached, hasCached := s.respCache[key]
		s.mu.Unlock()
		if dup {
			// Idempotent replay: never re-execute the handler; resend
			// the cached response if we still have it.
			if hasCached {
				_ = s.transport.Send(cached.data, addr)
			}
			return
		}
		s.persist()
	}

	resp := s.dispatch(msg, addr)

	if confirmable {
		resp.Type = ACK
		resp.MessageID = msg.MessageID
		data, err := Encode(resp)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.respCache[key] = respCacheEntry{data: data, at: time.Now()}
		s.purgeRespCacheLocked()
		s.mu.Unlock()
		_ = s.transport.Send(data, addr)
	} else {
		resp.Type = NON
		s.mu.Lock()
		s.nextMID++
		resp.MessageID = s.nextMID
		s.mu.Unlock()
		if data, err := Encode(resp); err == nil {
			_ = s.transport.Send(data, addr)
		}
	}
}

// dispatch routes the request to its handler, applying observe and
// block-wise semantics. The returned message is the response body.
func (s *Server) dispatch(req *Message, addr string) *Message {
	path := req.Path()

	// Observe registration / cancellation.
	if obsVal, ok := req.GetUint(OptionObserve); ok && req.Code == CodeGET {
		if obsVal == 1 {
			s.mu.Lock()
			s.observers.remove(addr, req.Token)
			s.persistLocked()
			s.mu.Unlock()
			return &Message{Code: CodeContent}
		}
		// obsVal == 0: register after we know the path is servable.
		s.mu.Lock()
		_, servable := s.handlers[path]
		s.mu.Unlock()
		if !servable {
			return &Message{Code: CodeNotFound}
		}
		sub := s.observers.add(addr, path, req.Token)
		resp := s.invokeHandler(req, addr, path)
		s.mu.Lock()
		sub.Seq++
		resp.SetUint(OptionObserve, sub.Seq)
		s.persistLocked()
		s.mu.Unlock()
		return resp
	}

	// Block1: request payload arrives in blocks.
	if b1, ok := req.GetUint(OptionBlock1); ok {
		return s.handleBlock1(req, addr, path, DecodeBlock(b1))
	}

	resp := s.invokeHandler(req, addr, path)

	// Block2: response payload is served in blocks on demand.
	if b2, ok := req.GetUint(OptionBlock2); ok {
		bo := DecodeBlock(b2)
		if bo.SZX > MaxBlockSZX {
			bo.SZX = MaxBlockSZX
		}
		if int(bo.Num)*bo.Size() >= len(resp.Payload) && len(resp.Payload) > 0 {
			return &Message{Code: CodeBadRequest}
		}
		blocks := splitBlocks(resp.Payload, bo.SZX)
		if len(blocks) == 0 || int(bo.Num) >= len(blocks) {
			return &Message{Code: CodeBadRequest}
		}
		desc := blocks[bo.Num]
		out := &Message{Code: resp.Code, Payload: blockSlice(resp.Payload, desc)}
		out.SetUint(OptionBlock2, desc.Encode())
		return out
	}
	return resp
}

// handleBlock1 reassembles an inbound block-wise request payload.
func (s *Server) handleBlock1(req *Message, addr, path string, bo BlockOption) *Message {
	key := observeKey(addr, req.Token)
	s.mu.Lock()
	r, ok := s.blocks[key]
	if !ok || bo.Num == 0 {
		r = newBlockReassembler(bo.SZX)
		s.blocks[key] = r
	}
	s.mu.Unlock()

	full, complete, err := r.Add(bo, req.Payload)
	if err != nil {
		s.mu.Lock()
		delete(s.blocks, key)
		s.mu.Unlock()
		return &Message{Code: CodeBadRequest}
	}
	resp := &Message{Code: CodeChanged}
	resp.SetUint(OptionBlock1, bo.Encode())
	if !complete {
		return resp // 2.31 Continue equivalent: ask for next block
	}
	s.mu.Lock()
	delete(s.blocks, key)
	s.mu.Unlock()

	// Deliver the reassembled payload to the handler as one request.
	merged := *req
	merged.Payload = full
	merged.Remove(OptionBlock1)
	out := s.invokeHandler(&merged, addr, path)
	out.SetUint(OptionBlock1, bo.Encode())
	return out
}

func (s *Server) invokeHandler(req *Message, addr, path string) *Message {
	s.mu.Lock()
	h := s.handlers[path]
	s.mu.Unlock()
	if h == nil {
		return &Message{Code: CodeNotFound}
	}
	resp, err := h(req, addr)
	if err != nil {
		return &Message{Code: CodeInternalError}
	}
	if resp == nil {
		return &Message{Code: CodeChanged}
	}
	return resp
}

// purgeRespCacheLocked drops cached responses older than the dedup window.
func (s *Server) purgeRespCacheLocked() {
	now := time.Now()
	for k, e := range s.respCache {
		if now.Sub(e.at) > s.cfg.DedupWindow {
			delete(s.respCache, k)
		}
	}
}

// --- persistence ---

func (s *Server) persist() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistLocked()
}

func (s *Server) persistLocked() {
	if s.stateDir == "" {
		return
	}
	_ = os.MkdirAll(s.stateDir, 0o755)
	_ = os.WriteFile(filepath.Join(s.stateDir, "dedup.json"), s.dedup.Snapshot(), 0o644)
	_ = os.WriteFile(filepath.Join(s.stateDir, "observe.json"), s.observers.Snapshot(), 0o644)
}

func (s *Server) restore() {
	if data, err := os.ReadFile(filepath.Join(s.stateDir, "dedup.json")); err == nil {
		_ = s.dedup.Restore(data)
	}
	if data, err := os.ReadFile(filepath.Join(s.stateDir, "observe.json")); err == nil {
		_ = s.observers.Restore(data)
	}
}
