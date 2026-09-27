package coap

import (
	"errors"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Addr 标识一个通信端点（UDP 地址或内存管道名）。
type Addr string

// ErrClosed 表示连接已关闭。
var ErrClosed = errors.New("coap: transport closed")

// errReadTimeout 为读超时内部错误。
var errReadTimeout = errors.New("coap: read timeout")

// PacketConn 抽象数据报收发，屏蔽 UDP 与内存管道差异。
type PacketConn interface {
	// ReadFrom 读取一个数据报；返回数据、来源地址或错误。
	ReadFrom() (data []byte, from Addr, err error)
	// WriteTo 向指定地址发送一个数据报。
	WriteTo(data []byte, to Addr) error
	// SetReadDeadline 设置读超时，用于实现优雅关闭。
	SetReadDeadline(t time.Time) error
	// LocalAddr 返回本地地址。
	LocalAddr() Addr
	// Close 关闭连接，之后读写返回 ErrClosed。
	Close() error
}

// ---------------- 内存数据报交换中心 ----------------

// MemHub 为内存数据报交换中心，可在测试中注入丢包、乱序与重复。
// 所有故障注入只影响投递过程，不会破坏报文内容。
type MemHub struct {
	mu      sync.Mutex
	conns   map[Addr]*memEndpoint
	rnd     *rand.Rand
	loss    float64                               // 丢包率
	dup     float64                               // 重复投递率
	reorder float64                               // 乱序（延迟投递）率
	filter  func(from, to Addr, data []byte) bool // 返回 true 表示丢弃
	closed  bool
}

// NewMemHub 创建内存交换中心。
func NewMemHub() *MemHub {
	return &MemHub{
		conns: make(map[Addr]*memEndpoint),
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Endpoint 创建（或返回已存在的）绑定到指定名字的端点。
func (h *MemHub) Endpoint(name string) PacketConn {
	h.mu.Lock()
	defer h.mu.Unlock()
	a := Addr(name)
	if ep, ok := h.conns[a]; ok {
		return ep
	}
	ep := &memEndpoint{
		hub:  h,
		addr: a,
		in:   make(chan memPacket, 256),
		done: make(chan struct{}),
	}
	h.conns[a] = ep
	return ep
}

// SetLoss 设置全局丢包率（0~1），用于测试重传。
func (h *MemHub) SetLoss(rate float64) {
	h.mu.Lock()
	h.loss = rate
	h.mu.Unlock()
}

// SetDuplicate 设置报文被额外复制投递一次的概率（0~1）。
func (h *MemHub) SetDuplicate(rate float64) {
	h.mu.Lock()
	h.dup = rate
	h.mu.Unlock()
}

// SetReorder 设置乱序投递概率（0~1），通过随机延迟部分报文实现。
func (h *MemHub) SetReorder(rate float64) {
	h.mu.Lock()
	h.reorder = rate
	h.mu.Unlock()
}

// SetFilter 设置确定性投递过滤器：返回 true 的报文被丢弃。
// 用于需要精确控制丢包时序的测试；传 nil 清除。
func (h *MemHub) SetFilter(f func(from, to Addr, data []byte) bool) {
	h.mu.Lock()
	h.filter = f
	h.mu.Unlock()
}

// deliver 将报文投递到目标端点（应用故障注入）。
func (h *MemHub) deliver(from, to Addr, data []byte) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	ep, ok := h.conns[to]
	loss, dup, reorder := h.loss, h.dup, h.reorder
	filter := h.filter
	r1, r2, r3 := h.rnd.Float64(), h.rnd.Float64(), h.rnd.Float64()
	delay := time.Duration(1+h.rnd.Intn(5)) * time.Millisecond
	h.mu.Unlock()

	if !ok || r1 < loss {
		return // 目标不存在或丢包
	}
	if filter != nil && filter(from, to, data) {
		return // 过滤器丢弃
	}
	pkt := memPacket{from: from, data: append([]byte(nil), data...)}
	if r3 < reorder {
		// 延迟投递以制造乱序。
		time.AfterFunc(delay, func() { ep.enqueue(pkt) })
	} else {
		ep.enqueue(pkt)
	}
	if r2 < dup {
		ep.enqueue(pkt) // 重复投递同一报文副本
	}
}

type memPacket struct {
	from Addr
	data []byte
}

type memEndpoint struct {
	hub  *MemHub
	addr Addr
	in   chan memPacket
	done chan struct{}

	deadlineMu sync.Mutex
	deadline   time.Time

	closeOnce sync.Once
}

func (e *memEndpoint) enqueue(p memPacket) {
	select {
	case e.in <- p:
	case <-e.done:
	default:
		// 缓冲满：模拟网络拥塞丢包。
	}
}

func (e *memEndpoint) ReadFrom() ([]byte, Addr, error) {
	e.deadlineMu.Lock()
	dl := e.deadline
	e.deadlineMu.Unlock()

	var timer *time.Timer
	var timeout <-chan time.Time
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return nil, "", errReadTimeout
		}
		timer = time.NewTimer(d)
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case p := <-e.in:
		return p.data, p.from, nil
	case <-timeout:
		return nil, "", errReadTimeout
	case <-e.done:
		return nil, "", ErrClosed
	}
}

func (e *memEndpoint) WriteTo(data []byte, to Addr) error {
	select {
	case <-e.done:
		return ErrClosed
	default:
	}
	e.hub.deliver(e.addr, to, data)
	return nil
}

func (e *memEndpoint) SetReadDeadline(t time.Time) error {
	e.deadlineMu.Lock()
	e.deadline = t
	e.deadlineMu.Unlock()
	return nil
}

func (e *memEndpoint) LocalAddr() Addr { return e.addr }

func (e *memEndpoint) Close() error {
	e.closeOnce.Do(func() {
		close(e.done)
		e.hub.mu.Lock()
		delete(e.hub.conns, e.addr)
		e.hub.mu.Unlock()
	})
	return nil
}

// ---------------- UDP 端点 ----------------

// UDPConn 基于真实 UDP 的 PacketConn 实现（本地回环测试可用）。
type UDPConn struct {
	c *net.UDPConn
}

// ListenUDP 在指定地址（如 "127.0.0.1:0"）创建 UDP 端点。
func ListenUDP(addr string) (PacketConn, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	return &UDPConn{c: c}, nil
}

func (u *UDPConn) ReadFrom() ([]byte, Addr, error) {
	buf := make([]byte, 64*1024)
	n, from, err := u.c.ReadFromUDP(buf)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			return nil, "", ErrClosed
		}
		return nil, "", err
	}
	return buf[:n], Addr(from.String()), nil
}

func (u *UDPConn) WriteTo(data []byte, to Addr) error {
	ua, err := net.ResolveUDPAddr("udp", string(to))
	if err != nil {
		return err
	}
	_, err = u.c.WriteToUDP(data, ua)
	if errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return err
}

func (u *UDPConn) SetReadDeadline(t time.Time) error { return u.c.SetReadDeadline(t) }

func (u *UDPConn) LocalAddr() Addr { return Addr(u.c.LocalAddr().String()) }

func (u *UDPConn) Close() error { return u.c.Close() }
