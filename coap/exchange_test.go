package coap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newPair 创建通过内存中心互联的客户端/服务器对。
func newPair(t *testing.T, hub *MemHub, handler Handler) (*Client, *Server) {
	t.Helper()
	if hub == nil {
		hub = NewMemHub()
	}
	srvConn := hub.Endpoint("srv")
	srv := NewServer(srvConn, ServerConfig{}, handler)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	cliConn := hub.Endpoint("cli")
	cli := NewClient(cliConn, ClientConfig{
		AckTimeout:      50 * time.Millisecond,
		AckRandomFactor: 1.0, // 测试中去随机化，超时确定
		MaxRetransmit:   4,
	})
	t.Cleanup(func() {
		cli.Close()
		srv.Shutdown(context.Background())
	})
	return cli, srv
}

func echoHandler(w ResponseWriter, r *Request) {
	_ = w.Write(&Message{Code: Content, Payload: append([]byte("echo:"), r.Msg.Payload...)})
}

func TestBasicExchange(t *testing.T) {
	cli, _ := newPair(t, nil, HandlerFunc(echoHandler))
	req := &Message{Code: POST, Payload: []byte("hello")}
	resp, err := cli.Do(context.Background(), "srv", req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Msg.Payload) != "echo:hello" {
		t.Fatalf("unexpected payload: %q", resp.Msg.Payload)
	}
}

func TestRetransmissionOnLoss(t *testing.T) {
	hub := NewMemHub()
	var sent atomic.Int32
	// 丢弃客户端发出的前 2 个报文（首发 + 第 1 次重传）。
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "cli" && to == "srv" {
			return sent.Add(1) <= 2
		}
		return false
	})
	cli, _ := newPair(t, hub, HandlerFunc(echoHandler))

	start := time.Now()
	req := &Message{Code: POST, Payload: []byte("x")}
	resp, err := cli.Do(context.Background(), "srv", req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Msg.Payload) != "echo:x" {
		t.Fatalf("unexpected payload: %q", resp.Msg.Payload)
	}
	// 至少经历了两次指数退避（50ms + 100ms）。
	if d := time.Since(start); d < 140*time.Millisecond {
		t.Fatalf("returned too fast, backoff not applied: %v", d)
	}
	if n := sent.Load(); n != 3 {
		t.Fatalf("expected 3 sends (1+2 retransmits), got %d", n)
	}
}

func TestTimeoutAfterMaxRetransmit(t *testing.T) {
	hub := NewMemHub()
	hub.SetLoss(1.0) // 全丢：服务器不可达
	cli, _ := newPair(t, hub, HandlerFunc(echoHandler))

	req := &Message{Code: GET}
	_, err := cli.Do(context.Background(), "srv", req)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	// 不得留下悬挂的请求状态。
	cli.mu.Lock()
	nPending := len(cli.pending)
	nByMID := len(cli.byMID)
	cli.mu.Unlock()
	if nPending != 0 || nByMID != 0 {
		t.Fatalf("dangling state: pending=%d byMID=%d", nPending, nByMID)
	}
}

func TestRetransmitCountRespected(t *testing.T) {
	hub := NewMemHub()
	var sent atomic.Int32
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "cli" {
			sent.Add(1)
			return true // 全部丢弃
		}
		return false
	})
	cli, _ := newPair(t, hub, HandlerFunc(echoHandler))

	_, err := cli.Do(context.Background(), "srv", &Message{Code: GET})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	// 首发 + MaxRetransmit(4) 次重传 = 5 次发送。
	if n := sent.Load(); n != 5 {
		t.Fatalf("expected 5 sends, got %d", n)
	}
}

func TestContextCancel(t *testing.T) {
	hub := NewMemHub()
	hub.SetLoss(1.0)
	cli, _ := newPair(t, hub, HandlerFunc(echoHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := cli.Do(ctx, "srv", &Message{Code: GET})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ctx deadline, got %v", err)
	}
	cli.mu.Lock()
	n := len(cli.pending)
	cli.mu.Unlock()
	if n != 0 {
		t.Fatalf("dangling pending after cancel: %d", n)
	}
}

func TestResetExchange(t *testing.T) {
	hub := NewMemHub()
	srvConn := hub.Endpoint("srv")
	// 手工服务器：对任何 CON 请求回 RST。
	go func() {
		for {
			data, from, err := srvConn.ReadFrom()
			if err != nil {
				return
			}
			msg, err := Decode(data)
			if err != nil || msg.Type != CON {
				continue
			}
			rst := &Message{Type: RST, Code: CodeEmpty, MessageID: msg.MessageID}
			if enc, err := rst.Encode(); err == nil {
				_ = srvConn.WriteTo(enc, from)
			}
		}
	}()
	cliConn := hub.Endpoint("cli")
	cli := NewClient(cliConn, ClientConfig{AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1.0})
	defer cli.Close()

	_, err := cli.Do(context.Background(), "srv", &Message{Code: GET})
	if !errors.Is(err, ErrReset) {
		t.Fatalf("want ErrReset, got %v", err)
	}
}

func TestMalformedPacketIgnoredByServer(t *testing.T) {
	hub := NewMemHub()
	cli, srv := newPair(t, nil, HandlerFunc(echoHandler))
	_ = cli
	_ = srv

	// 直接向服务器发送各种畸形报文：服务器不得崩溃或状态错乱。
	raw := hub.Endpoint("raw")
	defer raw.Close()
	malformed := [][]byte{
		{},
		{0x00},
		{0x80, 0x01, 0x00, 0x01},             // 版本错误
		{0x4F, 0x01, 0x00, 0x01},             // TKL 非法
		{0x40, 0x01, 0x00, 0x01, 0xFF},       // 裸 payload marker
		{0x40, 0x01, 0x00, 0x01, 0xF1, 0x00}, // 选项 delta=15
		{0x40, 0x01, 0x00, 0x01, 0x15, 0x01, 0x02}, // 选项截断
	}
	for _, m := range malformed {
		_ = raw.WriteTo(m, "srv")
	}
	// 服务器仍应正常服务。
	req := &Message{Code: POST, Payload: []byte("ok")}
	resp, err := cli.Do(context.Background(), "srv", req)
	if err != nil {
		t.Fatalf("server broken after malformed packets: %v", err)
	}
	if string(resp.Msg.Payload) != "echo:ok" {
		t.Fatalf("unexpected: %q", resp.Msg.Payload)
	}
}
