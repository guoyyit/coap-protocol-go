package coap

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// ErrServerClosed 表示服务器已关闭。
var ErrServerClosed = errors.New("coap: server closed")

// Request 为到达服务器的请求。
type Request struct {
	Msg  *Message
	From Addr
}

// ResponseWriter 供处理器写回响应。
type ResponseWriter interface {
	// Write 写回一条响应报文（Type/MessageID/Token 由服务器填充）。
	Write(resp *Message) error
}

// Handler 处理请求。
type Handler interface {
	ServeCOAP(w ResponseWriter, r *Request)
}

// HandlerFunc 适配函数为 Handler。
type HandlerFunc func(w ResponseWriter, r *Request)

// ServeCOAP 调用 f(w, r)。
func (f HandlerFunc) ServeCOAP(w ResponseWriter, r *Request) { f(w, r) }

// ServerConfig 为服务器参数。
type ServerConfig struct {
	// Dedup 为去重窗口配置。
	Dedup DedupConfig
	// MaxMessageSize 为单报文上限（默认 1024）。
	MaxMessageSize int
	// StateDir 为崩溃恢复状态目录；为空则不持久化。
	StateDir string
}

func (c ServerConfig) withDefaults() ServerConfig {
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = 1024
	}
	c.Dedup = c.Dedup.withDefaults()
	return c
}

// block1State 为服务器侧 Block1 组装状态。
type block1State struct {
	asm *blockAssembler
	at  time.Time
}

// cachedResponse 为 Block2 响应缓存。
type cachedResponse struct {
	code    Code
	options Options
	payload []byte
	at      time.Time
}

// notifRef 记录已发送通知与其订阅的对应关系（处理 ACK/RST）。
type notifRef struct {
	peer  Addr
	token []byte
}

// Server 为 CoAP 服务器。
type Server struct {
	conn    PacketConn
	cfg     ServerConfig
	handler Handler
	dedup   *Dedup
	obs     *observeRegistry

	midSeq uint32

	block1Mu sync.Mutex
	block1   map[string]*block1State

	block2Mu sync.Mutex
	block2   map[string]*cachedResponse

	notifMu sync.Mutex
	notif   map[uint16]notifRef

	mu      sync.Mutex
	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup

	persistMu sync.Mutex
}

// NewServer 创建服务器；handler 处理普通请求。
func NewServer(conn PacketConn, cfg ServerConfig, handler Handler) *Server {
	var seed [4]byte
	_, _ = rand.Read(seed[:])
	cfg = cfg.withDefaults()
	s := &Server{
		conn:    conn,
		cfg:     cfg,
		handler: handler,
		dedup:   NewDedup(cfg.Dedup),
		obs:     newObserveRegistry(),
		midSeq:  binary.BigEndian.Uint32(seed[:]),
		block1:  make(map[string]*block1State),
		block2:  make(map[string]*cachedResponse),
		notif:   make(map[uint16]notifRef),
		closeCh: make(chan struct{}),
	}
	if cfg.StateDir != "" {
		if snap, err := loadState(cfg.StateDir); err == nil && snap != nil {
			s.dedup.restore(snap.Dedup)
			s.obs.restore(snap.Observe)
		}
	}
	return s
}

// Start 启动接收循环（非阻塞）。
func (s *Server) Start() error {
	s.wg.Add(1)
	go s.readLoop()
	return nil
}

// Shutdown 停止接收并等待在途处理结束。
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.closeCh)
	_ = s.conn.Close()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// nextMID 生成服务器主动报文（通知）的消息编号。
func (s *Server) nextMID() uint16 {
	s.mu.Lock()
	s.midSeq++
	mid := uint16(s.midSeq)
	s.mu.Unlock()
	return mid
}

// readLoop 为服务器接收循环。
func (s *Server) readLoop() {
	defer s.wg.Done()
	for {
		data, from, err := s.conn.ReadFrom()
		if err != nil {
			return
		}
		msg, err := Decode(data)
		if err != nil {
			continue // 畸形报文：明确拒绝（丢弃），不影响状态
		}
		s.handleMessage(from, msg)
	}
}

// handleMessage 按类型处理入方向报文。
func (s *Server) handleMessage(from Addr, msg *Message) {
	switch msg.Type {
	case CON:
		if !msg.Code.IsRequest() {
			return
		}
		// 幂等去重：同一（来源，消息编号）只处理一次；
		// 重复投递时直接重发缓存响应。
		if dup, cached := s.dedup.Check(from, msg.MessageID); dup {
			if cached != nil {
				s.sendRaw(from, cached)
			}
			return
		}
		resp := s.processRequest(from, msg)
		if resp == nil {
			s.dedup.Store(from, msg.MessageID, nil)
			return
		}
		resp.Type = ACK
		resp.MessageID = msg.MessageID
		s.dedup.Store(from, msg.MessageID, resp)
		s.persist()
		s.sendRaw(from, resp)
	case NON:
		if !msg.Code.IsRequest() {
			return
		}
		resp := s.processRequest(from, msg)
		if resp == nil {
			return
		}
		resp.Type = NON
		resp.MessageID = s.nextMID()
		s.sendRaw(from, resp)
	case ACK:
		// 通知的 ACK：投递成功，清理对应关系。
		s.notifMu.Lock()
		delete(s.notif, msg.MessageID)
		s.notifMu.Unlock()
	case RST:
		// 通知被 RST：订阅方拒绝，注销订阅。
		s.notifMu.Lock()
		ref, ok := s.notif[msg.MessageID]
		delete(s.notif, msg.MessageID)
		s.notifMu.Unlock()
		if ok && ref.peer == from {
			if s.obs.deregister(ref.peer, ref.token) {
				s.persist()
			}
		}
	}
}

// processRequest 构造响应内容（不含 Type/MessageID 封装）。
func (s *Server) processRequest(from Addr, msg *Message) *Message {
	// 观察注销：GET + Observe:1。
	if obsVal, ok := msg.Options.GetUint(OptionObserve); ok && msg.Code == GET {
		if obsVal == 1 {
			if s.obs.deregister(from, msg.Token) {
				s.persist()
			}
			resp := &Message{Code: Content, Token: msg.Token}
			return resp
		}
		if obsVal == 0 {
			return s.processObserveRegister(from, msg)
		}
	}
	// Block1 分块上传。
	if b, ok := blockOption(msg.Options, OptionBlock1); ok {
		return s.processBlock1(from, msg, b)
	}
	// Block2 后续块拉取。
	if b, ok := blockOption(msg.Options, OptionBlock2); ok {
		return s.serveBlock2(from, msg, b)
	}
	resp := s.dispatchToHandler(from, msg)
	return s.maybeSplitBlock2(from, msg, resp)
}

// processObserveRegister 处理观察订阅（GET + Observe:0）。
func (s *Server) processObserveRegister(from Addr, msg *Message) *Message {
	seq := s.obs.register(from, msg.Token, msg.PathString())
	s.persist()
	resp := s.dispatchToHandler(from, msg)
	resp.Options.SetUint(OptionObserve, seq) // 注册响应携带当前顺序号
	return s.maybeSplitBlock2(from, msg, resp)
}

// dispatchToHandler 调用应用处理器。
func (s *Server) dispatchToHandler(from Addr, msg *Message) *Message {
	if s.handler == nil {
		return &Message{Code: NotFound, Token: msg.Token}
	}
	w := &responseWriter{}
	s.handler.ServeCOAP(w, &Request{Msg: msg, From: from})
	if w.resp == nil {
		return &Message{Code: Content, Token: msg.Token}
	}
	resp := w.resp
	resp.Token = msg.Token
	if !resp.Code.IsResponse() {
		resp.Code = Content
	}
	return resp
}

// responseWriter 收集处理器写回的响应。
type responseWriter struct {
	resp *Message
}

func (w *responseWriter) Write(resp *Message) error {
	w.resp = resp.Clone()
	return nil
}

// processBlock1 处理 Block1 分块；中间块回 2.31，末块组装完整后分发。
func (s *Server) processBlock1(from Addr, msg *Message, b Block) *Message {
	key := string(from) + "\x00" + string(msg.Token)
	s.block1Mu.Lock()
	st, ok := s.block1[key]
	if !ok || st.asm.size != b.Size {
		st = &block1State{asm: newBlockAssembler(b.Size)}
		s.block1[key] = st
	}
	st.at = time.Now()
	full, done, err := st.asm.add(b, msg.Payload)
	if done {
		delete(s.block1, key)
	}
	s.block1Mu.Unlock()

	if err != nil {
		resp := &Message{Code: BadRequest, Token: msg.Token}
		return resp
	}
	if !done {
		// 中间块：2.31 Continue，回显 Block1。
		resp := &Message{Code: Continue, Token: msg.Token}
		setBlock(&resp.Options, OptionBlock1, b)
		return resp
	}
	// 组装完成：以完整负载调用处理器。
	complete := msg.Clone()
	complete.Payload = full
	complete.Options.Remove(OptionBlock1)
	resp := s.dispatchToHandler(from, complete)
	return s.maybeSplitBlock2(from, msg, resp)
}

// maybeSplitBlock2 响应过大时切块：发送首块并缓存完整响应。
func (s *Server) maybeSplitBlock2(from Addr, req, resp *Message) *Message {
	if len(resp.Payload) <= s.cfg.MaxMessageSize {
		return resp
	}
	bs := blockSizeFor(s.cfg.MaxMessageSize - 64)
	key := string(from) + "\x00" + string(req.Token)
	s.block2Mu.Lock()
	s.block2[key] = &cachedResponse{
		code:    resp.Code,
		options: resp.Options,
		payload: resp.Payload,
		at:      time.Now(),
	}
	s.block2Mu.Unlock()

	first := resp.Clone()
	first.Payload = resp.Payload[:bs]
	setBlock(&first.Options, OptionBlock2, Block{Num: 0, More: true, Size: bs})
	first.Options.SetUint(OptionSize2, uint32(len(resp.Payload)))
	return first
}

// serveBlock2 处理 Block2 后续块请求。
func (s *Server) serveBlock2(from Addr, msg *Message, b Block) *Message {
	key := string(from) + "\x00" + string(msg.Token)
	s.block2Mu.Lock()
	cr, ok := s.block2[key]
	s.block2Mu.Unlock()
	if !ok {
		return &Message{Code: RequestEntityIncomplete, Token: msg.Token}
	}
	start := int(b.Num) * b.Size
	if start < 0 || start >= len(cr.payload) {
		return &Message{Code: BadRequest, Token: msg.Token}
	}
	end := start + b.Size
	if end > len(cr.payload) {
		end = len(cr.payload)
	}
	resp := &Message{Code: cr.code, Token: msg.Token, Options: append(Options(nil), cr.options...)}
	resp.Options.Remove(OptionObserve)
	resp.Payload = cr.payload[start:end]
	setBlock(&resp.Options, OptionBlock2, Block{Num: b.Num, More: end < len(cr.payload), Size: b.Size})
	resp.Options.SetUint(OptionSize2, uint32(len(cr.payload)))
	if end >= len(cr.payload) {
		s.block2Mu.Lock()
		delete(s.block2, key)
		s.block2Mu.Unlock()
	}
	return resp
}

// sendRaw 编码并发送报文。
func (s *Server) sendRaw(to Addr, msg *Message) {
	data, err := msg.Encode()
	if err != nil {
		return
	}
	_ = s.conn.WriteTo(data, to)
}

// Notify 向所有订阅指定路径的观察者推送一次通知。
// 通知为 CON 报文，携带单调递增的 Observe 顺序号。
func (s *Server) Notify(path string, payload []byte) {
	for _, sub := range s.obs.forPath(path) {
		seq := s.obs.nextSeq(sub.peer, sub.token)
		n := &Message{
			Type:    CON,
			Code:    Content,
			Token:   append([]byte(nil), sub.token...),
			Payload: payload,
		}
		n.MessageID = s.nextMID()
		n.Options.SetUint(OptionObserve, seq)
		s.notifMu.Lock()
		s.notif[n.MessageID] = notifRef{peer: sub.peer, token: append([]byte(nil), sub.token...)}
		s.notifMu.Unlock()
		s.sendRaw(sub.peer, n)
	}
	s.persist()
}

// persist 持久化去重与订阅状态（配置 StateDir 时）。
func (s *Server) persist() {
	if s.cfg.StateDir == "" {
		return
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	snap := &stateSnapshot{
		Dedup:   s.dedup.snapshot(),
		Observe: s.obs.snapshot(),
	}
	_ = saveState(s.cfg.StateDir, snap)
}
