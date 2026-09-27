package coap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fastConfig shortens timers so tests run quickly.
func fastConfig() ClientConfig {
	cfg := DefaultClientConfig()
	cfg.AckTimeout = 30 * time.Millisecond
	cfg.MaxRetransmit = 2
	cfg.MaxLatency = 500 * time.Millisecond
	return cfg
}

func echoHandler(req *Message, addr string) (*Message, error) {
	return &Message{Code: CodeContent, Payload: append([]byte("echo:"), req.Token...)}, nil
}

func newTestPair(t *testing.T, cfg PipeConfig) (*Client, *Server) {
	t.Helper()
	ct, st := NewPipe("client", "server", cfg)
	srv := NewServer(st, DefaultServerConfig(), "")
	srv.Handle("/echo", echoHandler)
	cli := NewClient(ct, fastConfig())
	t.Cleanup(func() {
		cli.Close()
		srv.Close()
	})
	return cli, srv
}

func TestBasicRequestResponse(t *testing.T) {
	cli, _ := newTestPair(t, PipeConfig{})
	req := &Message{Code: CodeGET}
	req.SetPath("/echo")
	resp, err := cli.Do(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Code != CodeContent {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestRetransmitAfterLoss(t *testing.T) {
	var sent, dropped int64
	cfg := PipeConfig{
		Drop: func(data []byte, from, to string) bool {
			if from != "client" {
				return false
			}
			n := atomic.AddInt64(&sent, 1)
			if n <= 2 { // drop the first two transmissions
				atomic.AddInt64(&dropped, 1)
				return true
			}
			return false
		},
	}
	cli, _ := newTestPair(t, cfg)
	req := &Message{Code: CodeGET}
	req.SetPath("/echo")
	start := time.Now()
	resp, err := cli.Do(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Code != CodeContent {
		t.Fatalf("code = %d", resp.Code)
	}
	if atomic.LoadInt64(&dropped) != 2 {
		t.Fatalf("expected 2 retransmissions, dropped=%d", dropped)
	}
	// Exponential backoff: 30ms + 60ms before third attempt.
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("retransmission too fast (%v), backoff not applied", elapsed)
	}
}

func TestTimeoutAfterMaxRetransmit(t *testing.T) {
	cfg := PipeConfig{
		Drop: func(data []byte, from, to string) bool { return from == "client" },
	}
	cli, _ := newTestPair(t, cfg)
	req := &Message{Code: CodeGET}
	req.SetPath("/echo")
	_, err := cli.Do(context.Background(), req, "server")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	// No dangling request state may remain.
	cli.mu.Lock()
	nPend, nMID := len(cli.pending), len(cli.byMID)
	cli.mu.Unlock()
	if nPend != 0 || nMID != 0 {
		t.Fatalf("dangling state: pending=%d byMID=%d", nPend, nMID)
	}
}

func TestDuplicateAckIgnored(t *testing.T) {
	cfg := PipeConfig{
		Duplicate: func(data []byte, from, to string) bool {
			return from == "server" // every response delivered twice
		},
	}
	cli, _ := newTestPair(t, cfg)
	req := &Message{Code: CodeGET}
	req.SetPath("/echo")
	resp, err := cli.Do(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Code != CodeContent {
		t.Fatalf("code = %d", resp.Code)
	}
	// Give the duplicate ACK time to arrive; state machine must stay clean.
	time.Sleep(50 * time.Millisecond)
	cli.mu.Lock()
	n := len(cli.pending)
	cli.mu.Unlock()
	if n != 0 {
		t.Fatalf("duplicate ACK corrupted state: pending=%d", n)
	}
}

func TestConcurrentOutOfOrderResponses(t *testing.T) {
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	// Handler echoes the token back, delaying inversely to the token's
	// last byte so responses come back out of order.
	srv.Handle("/echo", func(req *Message, addr string) (*Message, error) {
		delay := time.Duration(255-req.Token[len(req.Token)-1]) * time.Millisecond / 4
		time.Sleep(delay)
		return &Message{Code: CodeContent, Payload: req.Token}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := &Message{Code: CodeGET}
			req.SetPath("/echo")
			resp, err := cli.Do(context.Background(), req, "server")
			if err != nil {
				errs <- err
				return
			}
			// The response payload must be this request's own token.
			if string(resp.Payload) != string(req.Token) {
				errs <- fmt.Errorf("request %d got mismatched response", i)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestLateResponseAfterTokenReuseNotMisdelivered(t *testing.T) {
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	var firstRespData []byte
	var mu sync.Mutex
	srv.Handle("/echo", func(req *Message, addr string) (*Message, error) {
		resp := &Message{Code: CodeContent, Payload: []byte("first")}
		resp.Type = ACK
		resp.MessageID = req.MessageID
		resp.Token = req.Token
		data, _ := Encode(resp)
		mu.Lock()
		if firstRespData == nil {
			firstRespData = data
		}
		mu.Unlock()
		return &Message{Code: CodeContent, Payload: []byte("first")}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	token := []byte("reused-t")
	req1 := &Message{Code: CodeGET, Token: token}
	req1.SetPath("/echo")
	if _, err := cli.Do(context.Background(), req1, "server"); err != nil {
		t.Fatalf("first Do: %v", err)
	}

	// New request reusing the same token.
	srv.Handle("/echo", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: []byte("second")}, nil
	})
	req2 := &Message{Code: CodeGET, Token: token}
	req2.SetPath("/echo")
	result := make(chan *Message, 1)
	go func() {
		resp, err := cli.Do(context.Background(), req2, "server")
		if err != nil {
			t.Errorf("second Do: %v", err)
			return
		}
		result <- resp
	}()

	// Inject the late duplicate of the OLD response while the new
	// request is in flight. It must be ignored (unknown message ID).
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	stale := append([]byte(nil), firstRespData...)
	mu.Unlock()
	if err := ct.Send(stale, "server"); err == nil {
		// deliver as if the server re-sent it: push directly into client inbox
	}
	// Send from server side so Addr matches.
	_ = st.Send(stale, "client")

	select {
	case resp := <-result:
		if string(resp.Payload) != "second" {
			t.Fatalf("stale response misdelivered: got %q", resp.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second request never completed")
	}
}

// TestResetByPeer: a RST for an in-flight request fails it with ErrReset.
func TestResetByPeer(t *testing.T) {
	ct, st := NewPipe("client", "server", PipeConfig{})
	defer st.Close()
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	// Manual peer: read the request, then reject it with RST.
	go func() {
		p := <-st.Recv()
		m, err := Decode(p.Data)
		if err != nil {
			return
		}
		data, _ := Encode(&Message{Type: RST, Code: CodeEmpty, MessageID: m.MessageID})
		_ = st.Send(data, "client")
	}()

	req := &Message{Code: CodeGET}
	req.SetPath("/nowhere")
	_, err := cli.Do(context.Background(), req, "server")
	if !errors.Is(err, ErrReset) {
		t.Fatalf("got %v, want ErrReset", err)
	}
	// No dangling state after the reset.
	cli.mu.Lock()
	n := len(cli.pending)
	cli.mu.Unlock()
	if n != 0 {
		t.Fatalf("dangling state after RST: pending=%d", n)
	}
}

// TestStrayAckDoesNotDisturbInflight: ACKs with unknown message IDs
// (late, duplicated, or misdirected) must not affect in-flight requests.
func TestStrayAckDoesNotDisturbInflight(t *testing.T) {
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	midCh := make(chan uint16, 1)
	srv.Handle("/echo", func(req *Message, addr string) (*Message, error) {
		midCh <- req.MessageID            // report the in-flight message ID
		time.Sleep(60 * time.Millisecond) // keep the request in flight
		return &Message{Code: CodeContent, Payload: []byte("ok")}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	req := &Message{Code: CodeGET}
	req.SetPath("/echo")
	result := make(chan *Message, 1)
	go func() {
		resp, err := cli.Do(context.Background(), req, "server")
		if err != nil {
			t.Errorf("Do: %v", err)
			return
		}
		result <- resp
	}()
	mid := <-midCh
	// Inject stray ACKs with unknown message IDs while in flight.
	for _, mid := range []uint16{mid + 100, mid - 100, mid ^ 0xFFFF} {
		data, _ := Encode(&Message{Type: ACK, Code: CodeContent, MessageID: mid, Payload: []byte("bogus")})
		_ = st.Send(data, "client")
	}
	select {
	case resp := <-result:
		if string(resp.Payload) != "ok" {
			t.Fatalf("stray ACK corrupted matching: got %q", resp.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request never completed")
	}
}
