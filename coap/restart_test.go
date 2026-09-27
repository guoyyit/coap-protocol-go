package coap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestRestartDedupRecovery 验证服务器重启后，去重状态恢复：
// 历史重复报文不会被当作新请求再次处理，且能重发缓存响应。
func TestRestartDedupRecovery(t *testing.T) {
	hub := NewMemHub()
	dir := t.TempDir()

	// 捕获客户端请求的原始字节，用于稍后重放。
	reqBytes := make(chan []byte, 1)
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "cli" && to == "srv" {
			if m, err := Decode(data); err == nil && m.Code.IsRequest() {
				select {
				case reqBytes <- append([]byte(nil), data...):
				default:
				}
			}
		}
		return false
	})

	var calls atomic.Int32
	handler := HandlerFunc(func(w ResponseWriter, r *Request) {
		calls.Add(1)
		_ = w.Write(&Message{Code: Content, Payload: []byte("cached-resp")})
	})

	srv1 := NewServer(hub.Endpoint("srv"), ServerConfig{StateDir: dir}, handler)
	if err := srv1.Start(); err != nil {
		t.Fatal(err)
	}
	cli := NewClient(hub.Endpoint("cli"), ClientConfig{AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1.0})
	resp, err := cli.Do(context.Background(), "srv", &Message{Code: POST, Payload: []byte("q")})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Msg.Payload) != "cached-resp" {
		t.Fatalf("unexpected: %q", resp.Msg.Payload)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d", calls.Load())
	}
	captured := <-reqBytes
	cli.Close()
	srv1.Shutdown(context.Background())

	// 模拟崩溃重启：新服务器实例，相同状态目录。
	srv2 := NewServer(hub.Endpoint("srv"), ServerConfig{StateDir: dir}, handler)
	if err := srv2.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv2.Shutdown(context.Background())

	// 重放历史请求（同一来源、同一消息编号）。
	raw := hub.Endpoint("cli")
	defer raw.Close()
	if err := raw.WriteTo(captured, "srv"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, _, err := raw.ReadFrom()
	if err != nil {
		t.Fatalf("no cached response replayed: %v", err)
	}
	m, err := Decode(data)
	if err != nil {
		t.Fatalf("decode replayed response: %v", err)
	}
	if string(m.Payload) != "cached-resp" {
		t.Fatalf("unexpected replayed payload: %q", m.Payload)
	}
	// 关键断言：处理器未被再次调用（幂等跨重启保持）。
	if n := calls.Load(); n != 1 {
		t.Fatalf("handler called %d times after restart, want 1", n)
	}
}

// TestRestartObserveRecovery 验证观察订阅在重启后恢复：
// 通知继续推送且顺序号连续；订阅者失效（RST）后不再推送。
func TestRestartObserveRecovery(t *testing.T) {
	hub := NewMemHub()
	dir := t.TempDir()

	srv1 := NewServer(hub.Endpoint("srv"), ServerConfig{StateDir: dir}, HandlerFunc(observableHandler))
	if err := srv1.Start(); err != nil {
		t.Fatal(err)
	}
	cli := NewClient(hub.Endpoint("cli"), ClientConfig{AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1.0})
	defer cli.Close()

	obs, err := cli.Observe(context.Background(), "srv", "/res")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	first := recvN(t, obs.C, 1)
	srv1.Notify("/res", []byte("n1"))
	ns1 := recvN(t, obs.C, 1)
	lastSeq := ns1[0].Seq
	srv1.Shutdown(context.Background())

	// 重启：订阅从状态目录恢复。
	srv2 := NewServer(hub.Endpoint("srv"), ServerConfig{StateDir: dir}, HandlerFunc(observableHandler))
	if err := srv2.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv2.Shutdown(context.Background())
	if got := len(srv2.obs.forPath("/res")); got != 1 {
		t.Fatalf("subscription not restored, got %d", got)
	}

	srv2.Notify("/res", []byte("n2"))
	ns2 := recvN(t, obs.C, 1)
	if string(ns2[0].Msg.Payload) != "n2" {
		t.Fatalf("unexpected notification after restart: %q", ns2[0].Msg.Payload)
	}
	// 顺序号跨重启单调递增。
	if ns2[0].Seq <= lastSeq {
		t.Fatalf("seq not increasing across restart: %d -> %d", lastSeq, ns2[0].Seq)
	}
	_ = first

	// 订阅者失效：拦截自动 ACK，改回 RST。
	notifMID := make(chan uint16, 1)
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "srv" && to == "cli" {
			if m, err := Decode(data); err == nil && m.Type == CON && m.Code == Content {
				select {
				case notifMID <- m.MessageID:
				default:
				}
			}
			return false
		}
		if from == "cli" && to == "srv" {
			if m, err := Decode(data); err == nil && m.Type == ACK {
				return true
			}
		}
		return false
	})
	srv2.Notify("/res", []byte("n3"))
	_ = recvN(t, obs.C, 1)
	mid := <-notifMID
	cliConn := hub.Endpoint("cli")
	rst := &Message{Type: RST, Code: CodeEmpty, MessageID: mid}
	enc, _ := rst.Encode()
	_ = cliConn.WriteTo(enc, "srv")

	deadline := time.Now().Add(2 * time.Second)
	for len(srv2.obs.forPath("/res")) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(srv2.obs.forPath("/res")); got != 0 {
		t.Fatalf("dead subscription not removed: %d", got)
	}
	// 注销后的 Notify 不得再产生任何推送。
	hub.SetFilter(nil)
	sent := make(chan struct{}, 1)
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "srv" && to == "cli" {
			if m, err := Decode(data); err == nil && m.Type == CON {
				select {
				case sent <- struct{}{}:
				default:
				}
			}
		}
		return false
	})
	srv2.Notify("/res", []byte("n4"))
	select {
	case <-sent:
		t.Fatal("notification sent to dead subscription")
	case <-time.After(200 * time.Millisecond):
	}
}
