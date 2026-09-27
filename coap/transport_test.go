package coap

import (
	"testing"
	"time"
)

func TestMemHubSendRecv(t *testing.T) {
	h := NewMemHub()
	a := h.Endpoint("a")
	b := h.Endpoint("b")
	defer a.Close()
	defer b.Close()

	if err := a.WriteTo([]byte("ping"), "b"); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	data, from, err := b.ReadFrom()
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(data) != "ping" || from != "a" {
		t.Fatalf("got %q from %q", data, from)
	}
}

func TestMemHubLoss(t *testing.T) {
	h := NewMemHub()
	h.SetLoss(1.0)
	a := h.Endpoint("a")
	b := h.Endpoint("b")
	defer a.Close()
	defer b.Close()

	_ = a.WriteTo([]byte("x"), "b")
	_ = b.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if _, _, err := b.ReadFrom(); err == nil {
		t.Fatal("expected timeout under 100% loss")
	}
}

func TestMemHubDuplicate(t *testing.T) {
	h := NewMemHub()
	h.SetDuplicate(1.0)
	a := h.Endpoint("a")
	b := h.Endpoint("b")
	defer a.Close()
	defer b.Close()

	_ = a.WriteTo([]byte("x"), "b")
	for i := 0; i < 2; i++ {
		_ = b.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := b.ReadFrom(); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
}

func TestMemHubClosed(t *testing.T) {
	h := NewMemHub()
	a := h.Endpoint("a")
	_ = a.Close()
	if err := a.WriteTo([]byte("x"), "a"); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if _, _, err := a.ReadFrom(); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestUDPLoopback(t *testing.T) {
	a, err := ListenUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer a.Close()
	b, err := ListenUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer b.Close()

	if err := a.WriteTo([]byte("udp-ping"), b.LocalAddr()); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, from, err := b.ReadFrom()
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(data) != "udp-ping" || from != a.LocalAddr() {
		t.Fatalf("got %q from %q", data, from)
	}
}
