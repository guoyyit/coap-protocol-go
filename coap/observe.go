package coap

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrObserveCancelled 表示观察已被取消。
var ErrObserveCancelled = errors.New("coap: observation cancelled")

// ErrNotObservable 表示对端不接受观察（响应未携带 Observe 选项）。
var ErrNotObservable = errors.New("coap: resource not observable")

// Notification 为一次观察推送。
type Notification struct {
	Msg *Message
	// Seq 为 Observe 选项中的顺序号，用于检测乱序与丢失。
	Seq uint32
}

// observationState 为客户端侧订阅的内部状态。
type observationState struct {
	token   string
	peer    Addr
	path    string
	ch      chan Notification
	lastSeq uint32
	hasSeq  bool
	closed  bool
	mu      sync.Mutex
	onClose func(token string)
}

// deliver 向订阅者投递通知；重复顺序号被去重。
func (o *observationState) deliver(n Notification) {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	if o.hasSeq && n.Seq == o.lastSeq {
		o.mu.Unlock()
		return // 重复通知：幂等丢弃
	}
	o.lastSeq = n.Seq
	o.hasSeq = true
	ch := o.ch
	o.mu.Unlock()
	select {
	case ch <- n:
	default:
		// 订阅者消费过慢：丢弃最旧语义之外的新通知，避免阻塞协议循环。
	}
}

func (o *observationState) close() {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.closed = true
	close(o.ch)
	o.mu.Unlock()
	if o.onClose != nil {
		o.onClose(o.token)
	}
}

// Observation 为客户端侧的一条订阅。
type Observation struct {
	// C 推送通知通道；订阅取消或连接关闭后关闭。
	C <-chan Notification

	state *observationState
}

// Cancel 取消订阅；向服务器发送 Observe:1 注销请求，
// 之后服务器不得继续推送。幂等。
func (o *Observation) Cancel(ctx context.Context) error {
	if o == nil || o.state == nil {
		return nil
	}
	st := o.state
	st.close()
	return nil
}

// Observe 发起对资源的观察订阅（GET + Observe:0）。
// 首个响应到达后返回 Observation；后续通知经 Observation.C 投递。
func (c *Client) Observe(ctx context.Context, to Addr, path string) (*Observation, error) {
	req := &Message{Type: CON, Code: GET}
	req.Options.SetPath(path)
	req.Options.SetUint(OptionObserve, 0)
	req.Token = c.nextToken()

	st := &observationState{
		token: string(req.Token),
		peer:  to,
		path:  path,
		ch:    make(chan Notification, 16),
	}
	st.onClose = func(token string) {
		c.obsMu.Lock()
		delete(c.obs, token)
		c.obsMu.Unlock()
		// 发送注销请求（GET + Observe:1，同令牌），尽力而为。
		dereg := &Message{Type: CON, Code: GET, Token: req.Token}
		dereg.Options.SetPath(path)
		dereg.Options.SetUint(OptionObserve, 1)
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = c.roundTrip(ctx2, to, dereg)
	}

	c.obsMu.Lock()
	c.obs[st.token] = st
	c.obsMu.Unlock()

	resp, err := c.roundTrip(ctx, to, req)
	if err != nil {
		st.close()
		return nil, err
	}
	if !resp.Code.IsResponse() {
		st.close()
		return nil, ErrNotObservable
	}
	if _, ok := resp.Options.GetUint(OptionObserve); !ok {
		st.close()
		// 服务器不支持观察：把首个响应作为普通响应处理是允许的，
		// 但本实现明确报错，避免调用方误以为订阅成功。
		return nil, ErrNotObservable
	}
	// 注册响应本身也是第一条通知。
	st.deliver(Notification{Msg: resp, Seq: observeSeq(resp)})
	return &Observation{C: st.ch, state: st}, nil
}

// observeSeq 提取报文的 Observe 顺序号。
func observeSeq(m *Message) uint32 {
	if v, ok := m.Options.GetUint(OptionObserve); ok {
		return v
	}
	return 0
}

// handleNotification 尝试把响应作为观察通知投递；
// 返回 true 表示已按通知处理。
func (c *Client) handleNotification(from Addr, msg *Message) bool {
	if _, ok := msg.Options.Get(OptionObserve); !ok {
		return false
	}
	c.obsMu.Lock()
	st := c.obs[string(msg.Token)]
	c.obsMu.Unlock()
	if st == nil || st.peer != from {
		return false
	}
	st.deliver(Notification{Msg: msg, Seq: observeSeq(msg)})
	return true
}

// ---------------- 服务器侧订阅登记 ----------------

// subscription 为一条服务器侧订阅。
type subscription struct {
	peer  Addr
	token []byte
	path  string
	seq   uint32 // 单调递增的通知顺序号
}

// observeRegistry 为服务器侧订阅登记表。并发安全。
type observeRegistry struct {
	mu   sync.Mutex
	subs map[string]*subscription // key: peer + "\x00" + token
}

func newObserveRegistry() *observeRegistry {
	return &observeRegistry{subs: make(map[string]*subscription)}
}

func observeKey(peer Addr, token []byte) string {
	return string(peer) + "\x00" + string(token)
}

// register 登记或刷新订阅，返回当前通知顺序号。
func (r *observeRegistry) register(peer Addr, token []byte, path string) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := observeKey(peer, token)
	if s, ok := r.subs[k]; ok {
		s.path = path
		return s.seq
	}
	s := &subscription{peer: peer, token: append([]byte(nil), token...), path: path}
	r.subs[k] = s
	return s.seq
}

// deregister 移除订阅；返回是否存在。
func (r *observeRegistry) deregister(peer Addr, token []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := observeKey(peer, token)
	if _, ok := r.subs[k]; !ok {
		return false
	}
	delete(r.subs, k)
	return true
}

// nextSeq 返回订阅的下一个通知顺序号。
func (r *observeRegistry) nextSeq(peer Addr, token []byte) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.subs[observeKey(peer, token)]; ok {
		s.seq++
		return s.seq
	}
	return 0
}

// forPath 返回订阅指定路径的所有订阅快照。
func (r *observeRegistry) forPath(path string) []*subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*subscription
	for _, s := range r.subs {
		if s.path == path {
			cp := *s
			out = append(out, &cp)
		}
	}
	return out
}

// snapshot 导出订阅用于持久化。
func (r *observeRegistry) snapshot() []observeRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []observeRecord
	for _, s := range r.subs {
		out = append(out, observeRecord{
			Addr:  string(s.peer),
			Token: append([]byte(nil), s.token...),
			Path:  s.path,
			Seq:   s.seq,
		})
	}
	return out
}

// restore 从持久化记录重建订阅。
func (r *observeRegistry) restore(recs []observeRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range recs {
		k := observeKey(Addr(rec.Addr), rec.Token)
		if _, ok := r.subs[k]; ok {
			continue
		}
		r.subs[k] = &subscription{
			peer:  Addr(rec.Addr),
			token: append([]byte(nil), rec.Token...),
			path:  rec.Path,
			seq:   rec.Seq,
		}
	}
}
