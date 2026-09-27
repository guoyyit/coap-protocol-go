package coap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestDuplicateConProcessedOnce: a redelivered CON must execute the
// handler exactly once, and the duplicate gets the cached response.
func TestDuplicateConProcessedOnce(t *testing.T) {
	var calls int64
	cfg := PipeConfig{
		Duplicate: func(data []byte, from, to string) bool {
			return from == "client" // every request delivered twice
		},
	}
	ct, st := NewPipe("client", "server", cfg)
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	srv.Handle("/inc", func(req *Message, addr string) (*Message, error) {
		atomic.AddInt64(&calls, 1)
		return &Message{Code: CodeChanged}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	req := &Message{Code: CodePOST}
	req.SetPath("/inc")
	if _, err := cli.Do(context.Background(), req, "server"); err != nil {
		t.Fatalf("Do: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := atomic.LoadInt64(&calls); n != 1 {
		t.Fatalf("handler executed %d times, want 1", n)
	}
}

// TestMalformedPacketDoesNotCrashServer: garbage datagrams are dropped
// and the server keeps serving valid requests.
func TestMalformedPacketDoesNotCrashServer(t *testing.T) {
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	srv.Handle("/echo", echoHandler)
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	garbage := [][]byte{
		{},                             // empty
		{0xFF, 0xFF},                   // too short
		{0x40, 0x01, 0x00, 0x01, 0xFF}, // payload marker, no payload
		{0x49, 0x01, 0x00, 0x01},       // reserved TKL
		{0x40, 0x01, 0x00, 0x01, 0xF0}, // reserved delta
		{0xC0, 0x01, 0x00, 0x01},       // version 3
	}
	for _, g := range garbage {
		if err := ct.Send(g, "server"); err != nil {
			t.Fatalf("send garbage: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	req := &Message{Code: CodeGET}
	req.SetPath("/echo")
	resp, err := cli.Do(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("server broken after garbage: %v", err)
	}
	if resp.Code != CodeContent {
		t.Fatalf("code = %d", resp.Code)
	}
}

// TestCrashRecoveryDedup: after restart, a replayed pre-crash CON must
// not be processed again.
func TestCrashRecoveryDedup(t *testing.T) {
	dir := t.TempDir()
	var calls int64

	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), dir)
	srv.Handle("/inc", func(req *Message, addr string) (*Message, error) {
		atomic.AddInt64(&calls, 1)
		return &Message{Code: CodeChanged}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	req := &Message{Code: CodePOST, Token: []byte("tok-1")}
	req.SetPath("/inc")
	if _, err := cli.Do(context.Background(), req, "server"); err != nil {
		t.Fatalf("Do: %v", err)
	}
	sentMID := req.MessageID // Do assigns the wire message ID
	if n := atomic.LoadInt64(&calls); n != 1 {
		t.Fatalf("calls = %d, want 1", n)
	}
	srv.Close() // "crash": state persisted on Close

	// Restart with the same state dir.
	st2 := &PipeTransport{addr: "server", in: make(chan Packet, 256)}
	ct.mu.Lock()
	ct.peer = st2
	ct.mu.Unlock()
	st2.peer = ct
	srv2 := NewServer(st2, DefaultServerConfig(), dir)
	defer srv2.Close()
	srv2.Handle("/inc", func(req *Message, addr string) (*Message, error) {
		atomic.AddInt64(&calls, 1)
		return &Message{Code: CodeChanged}, nil
	})

	// Replay the exact pre-crash datagram (same addr, same message ID).
	replay := &Message{Type: CON, Code: CodePOST, Token: []byte("tok-1"), MessageID: sentMID}
	replay.SetPath("/inc")
	data, _ := Encode(replay)
	if err := ct.Send(data, "server"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := atomic.LoadInt64(&calls); n != 1 {
		t.Fatalf("historical duplicate processed after restart: calls = %d", n)
	}
}

// TestCrashRecoveryObserve: subscriptions survive a restart and keep
// their sequence numbers; the restarted server resumes notifications.
func TestCrashRecoveryObserve(t *testing.T) {
	dir := t.TempDir()
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), dir)
	srv.Handle("/temp", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: []byte("20")}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	req := &Message{Code: CodeGET}
	req.SetPath("/temp")
	obs, err := cli.Observe(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	n0 := recvNotification(t, obs, time.Second)
	srv.NotifyChanged("/temp", []byte("21"))
	n1 := recvNotification(t, obs, time.Second)
	srv.Close() // "crash"

	// Restart on the same state dir; subscriptions are restored.
	st2 := &PipeTransport{addr: "server", in: make(chan Packet, 256)}
	ct.mu.Lock()
	ct.peer = st2
	ct.mu.Unlock()
	st2.peer = ct
	srv2 := NewServer(st2, DefaultServerConfig(), dir)
	defer srv2.Close()
	srv2.Handle("/temp", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: []byte("20")}, nil
	})

	srv2.NotifyChanged("/temp", []byte("22"))
	n2 := recvNotification(t, obs, 2*time.Second)
	if n2.Seq <= n1.Seq || n2.Seq <= n0.Seq {
		t.Fatalf("sequence regressed after restart: %d -> %d", n1.Seq, n2.Seq)
	}
	if string(n2.Message.Payload) != "22" {
		t.Fatalf("payload = %q", n2.Message.Payload)
	}
}

// TestDedupSnapshotRestore exercises the dedup persistence round trip.
func TestDedupSnapshotRestore(t *testing.T) {
	d := NewDedup(time.Minute)
	k := DedupKey{Addr: "a", MessageID: 7}
	if d.Seen(k) {
		t.Fatal("first Seen must be false")
	}
	if !d.Seen(k) {
		t.Fatal("second Seen must be true")
	}
	snap := d.Snapshot()

	d2 := NewDedup(time.Minute)
	if err := d2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !d2.Seen(k) {
		t.Fatal("restored dedup lost the entry")
	}
	// A different key is still new.
	if d2.Seen(DedupKey{Addr: "a", MessageID: 8}) {
		t.Fatal("unknown key reported as duplicate")
	}
}
