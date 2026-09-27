package coap

import (
	"context"
	"testing"
	"time"
)

// TestUDPEndToEnd 在真实 UDP 回环上跑通请求/响应与观察推送。
func TestUDPEndToEnd(t *testing.T) {
	srvConn, err := ListenUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	srv := NewServer(srvConn, ServerConfig{}, HandlerFunc(func(w ResponseWriter, r *Request) {
		_ = w.Write(&Message{Code: Content, Payload: append([]byte("udp:"), r.Msg.Payload...)})
	}))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	cliConn, err := ListenUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	cli := NewClient(cliConn, ClientConfig{AckTimeout: 100 * time.Millisecond, AckRandomFactor: 1.0})
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, srvConn.LocalAddr(), &Message{Code: POST, Payload: []byte("ping")})
	if err != nil {
		t.Fatalf("Do over UDP: %v", err)
	}
	if string(resp.Msg.Payload) != "udp:ping" {
		t.Fatalf("unexpected: %q", resp.Msg.Payload)
	}

	// UDP 上的观察推送。
	obs, err := cli.Observe(ctx, srvConn.LocalAddr(), "/udp-res")
	if err != nil {
		t.Fatalf("Observe over UDP: %v", err)
	}
	_ = recvN(t, obs.C, 1)
	srv.Notify("/udp-res", []byte("udp-notify"))
	ns := recvN(t, obs.C, 1)
	if string(ns[0].Msg.Payload) != "udp-notify" {
		t.Fatalf("unexpected notification: %q", ns[0].Msg.Payload)
	}
	if err := obs.Cancel(ctx); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}
