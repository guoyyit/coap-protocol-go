package coap

import (
	"context"
	"testing"
	"time"
)

func newObservePair(t *testing.T, cfg PipeConfig) (*Client, *Server) {
	t.Helper()
	ct, st := NewPipe("client", "server", cfg)
	srv := NewServer(st, DefaultServerConfig(), "")
	srv.Handle("/temp", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: []byte("20")}, nil
	})
	cli := NewClient(ct, fastConfig())
	t.Cleanup(func() {
		cli.Close()
		srv.Close()
	})
	return cli, srv
}

func recvNotification(t *testing.T, obs *Observation, timeout time.Duration) Notification {
	t.Helper()
	select {
	case n, ok := <-obs.C:
		if !ok {
			t.Fatal("observation channel closed unexpectedly")
		}
		return n
	case <-time.After(timeout):
		t.Fatal("timed out waiting for notification")
		return Notification{}
	}
}

func TestObserveSubscribeNotifyCancel(t *testing.T) {
	cli, srv := newObservePair(t, PipeConfig{})
	req := &Message{Code: CodeGET}
	req.SetPath("/temp")
	obs, err := cli.Observe(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}

	// Registration response is the first notification.
	n0 := recvNotification(t, obs, time.Second)
	if string(n0.Message.Payload) != "20" {
		t.Fatalf("initial payload = %q", n0.Message.Payload)
	}

	// Server pushes state changes with increasing sequence numbers.
	srv.NotifyChanged("/temp", []byte("21"))
	n1 := recvNotification(t, obs, time.Second)
	srv.NotifyChanged("/temp", []byte("22"))
	n2 := recvNotification(t, obs, time.Second)

	if n1.Seq >= n2.Seq {
		t.Fatalf("sequence not increasing: %d then %d", n1.Seq, n2.Seq)
	}
	if string(n1.Message.Payload) != "21" || string(n2.Message.Payload) != "22" {
		t.Fatalf("payloads = %q, %q", n1.Message.Payload, n2.Message.Payload)
	}

	// After cancel, no further notifications are delivered; the
	// channel is closed (a closed channel is not a notification).
	obs.Cancel()
	time.Sleep(50 * time.Millisecond) // let the deregister reach the server
	srv.NotifyChanged("/temp", []byte("23"))
	select {
	case n, ok := <-obs.C:
		if ok {
			t.Fatalf("notification after cancel: %+v", n)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func TestObserveSequenceGapDetection(t *testing.T) {
	// Drop one notification to simulate loss; client must see a seq gap.
	var count int
	cfg := PipeConfig{
		Drop: func(data []byte, from, to string) bool {
			if from != "server" {
				return false
			}
			m, err := Decode(data)
			if err != nil || m.Type != CON {
				return false
			}
			if _, ok := m.GetUint(OptionObserve); !ok {
				return false
			}
			count++
			return count == 1 // drop the first push (seq 2)
		},
	}
	cli, srv := newObservePair(t, cfg)
	req := &Message{Code: CodeGET}
	req.SetPath("/temp")
	obs, err := cli.Observe(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	n0 := recvNotification(t, obs, time.Second) // initial, seq 1

	srv.NotifyChanged("/temp", []byte("21")) // seq 2: dropped
	srv.NotifyChanged("/temp", []byte("22")) // seq 3: delivered
	n := recvNotification(t, obs, 2*time.Second)
	// The jump from seq 1 to seq 3 reveals the lost notification.
	if n.Seq != n0.Seq+2 {
		t.Fatalf("expected seq %d after gap, got %d", n0.Seq+2, n.Seq)
	}
}

func TestServerStopsNotifyingDeadSubscriber(t *testing.T) {
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	srv.Handle("/temp", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: []byte("20")}, nil
	})
	cli := NewClient(ct, fastConfig())
	req := &Message{Code: CodeGET}
	req.SetPath("/temp")
	obs, err := cli.Observe(context.Background(), req, "server")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	recvNotification(t, obs, time.Second)

	// Client vanishes (no cancel, transport closed): server must not
	// keep the subscriber forever.
	cli.Close()
	srv.NotifyChanged("/temp", []byte("21")) // send fails silently
	// Expire the TTL and verify the subscriber is pruned.
	srv.mu.Lock()
	for _, o := range srv.observers.subs {
		o.LastSeen = o.LastSeen.Add(-2 * srv.cfg.ObserverTTL)
	}
	srv.mu.Unlock()
	srv.NotifyChanged("/temp", []byte("22"))
	srv.mu.Lock()
	n := len(srv.observers.subs)
	srv.mu.Unlock()
	if n != 0 {
		t.Fatalf("dead subscriber not pruned: %d remain", n)
	}
}
