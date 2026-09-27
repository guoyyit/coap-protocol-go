package coap

import "sync"

// Packet is one inbound datagram with its source address.
type Packet struct {
	Data []byte
	Addr string
}

// Transport is a datagram transport (UDP-like). Implementations must be
// safe for concurrent use.
type Transport interface {
	// Send transmits one datagram to addr.
	Send(data []byte, addr string) error
	// Recv returns the channel of inbound packets. Closed on Close.
	Recv() <-chan Packet
	// LocalAddr returns the local address identity of this transport.
	LocalAddr() string
	// Close shuts the transport down.
	Close() error
}

// PipeConfig tunes the in-memory pipe (loss/duplication simulation).
type PipeConfig struct {
	// Drop, if non-nil, decides per datagram whether to drop it (test hook).
	Drop func(data []byte, from, to string) bool
	// Duplicate, if non-nil, decides per datagram whether to deliver it twice.
	Duplicate func(data []byte, from, to string) bool
}

// PipeTransport is an in-memory Transport for tests and local use.
// Delivery is asynchronous and unordered, mirroring datagram networks.
type PipeTransport struct {
	mu     sync.Mutex
	addr   string
	peer   *PipeTransport
	in     chan Packet
	closed bool
	cfg    PipeConfig
}

// NewPipe creates a connected pair of in-memory transports.
func NewPipe(addrA, addrB string, cfg PipeConfig) (*PipeTransport, *PipeTransport) {
	a := &PipeTransport{addr: addrA, in: make(chan Packet, 256), cfg: cfg}
	b := &PipeTransport{addr: addrB, in: make(chan Packet, 256), cfg: cfg}
	a.peer, b.peer = b, a
	return a, b
}

func (t *PipeTransport) Send(data []byte, addr string) error {
	t.mu.Lock()
	peer := t.peer
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if peer == nil || addr != peer.addr {
		return ErrClosed
	}
	cp := append([]byte(nil), data...)
	if t.cfg.Drop != nil && t.cfg.Drop(cp, t.addr, addr) {
		return nil // silently lost, as datagrams do
	}
	deliver := func() {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		if peer.closed {
			return
		}
		select {
		case peer.in <- Packet{Data: cp, Addr: t.addr}:
		default: // peer buffer full: datagram dropped
		}
	}
	deliver()
	if t.cfg.Duplicate != nil && t.cfg.Duplicate(cp, t.addr, addr) {
		deliver()
	}
	return nil
}

func (t *PipeTransport) Recv() <-chan Packet { return t.in }
func (t *PipeTransport) LocalAddr() string   { return t.addr }

func (t *PipeTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.in)
	return nil
}
