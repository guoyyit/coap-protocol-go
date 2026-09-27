package coap

import (
	"context"
	"testing"
	"time"
)

func observableHandler(w ResponseWriter, r *Request) {
	_ = w.Write(&Message{Code: Content, Payload: []byte("v0")})
}

// recvN 从通知通道收取 n 条，超时失败。
func recvN(t *testing.T, ch <-chan Notification, n int) []Notification {
	t.Helper()
	var out []Notification
	for i := 0; i < n; i++ {
		select {
		case nt, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d notifications", len(out))
			}
			out = append(out, nt)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting notification %d (have %d)", i, len(out))
		}
	}
	return out
}

func TestObserveNotifyCancel(t *testing.T) {
	cli, srv := newPair(t, nil, HandlerFunc(observableHandler))

	obs, err := cli.Observe(context.Background(), "srv", "/sensors/temp")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	// 注册响应即第一条通知。
	first := recvN(t, obs.C, 1)
	if string(first[0].Msg.Payload) != "v0" {
		t.Fatalf("unexpected initial payload: %q", first[0].Msg.Payload)
	}

	// 服务器推送两次变更。
	srv.Notify("/sensors/temp", []byte("v1"))
	srv.Notify("/sensors/temp", []byte("v2"))
	ns := recvN(t, obs.C, 2)
	if string(ns[0].Msg.Payload) != "v1" || string(ns[1].Msg.Payload) != "v2" {
		t.Fatalf("unexpected notifications: %q %q", ns[0].Msg.Payload, ns[1].Msg.Payload)
	}
	// 顺序号单调递增，可用于检测乱序与丢失。
	if !(ns[0].Seq < ns[1].Seq) {
		t.Fatalf("observe seq not increasing: %d %d", ns[0].Seq, ns[1].Seq)
	}

	// 取消订阅。
	if err := obs.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	srv.Notify("/sensors/temp", []byte("v3"))
	select {
	case nt, ok := <-obs.C:
		if ok {
			t.Fatalf("notification after cancel: %q", nt.Msg.Payload)
		}
		// 通道关闭：符合预期。
	case <-time.After(300 * time.Millisecond):
		// 无推送：符合预期。
	}
	// 服务器侧订阅已移除。
	if got := len(srv.obs.forPath("/sensors/temp")); got != 0 {
		t.Fatalf("subscription not removed, %d left", got)
	}
}

func TestObserveDuplicateNotification(t *testing.T) {
	hub := NewMemHub()
	cli, srv := newPair(t, hub, HandlerFunc(observableHandler))

	obs, err := cli.Observe(context.Background(), "srv", "/res")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	_ = recvN(t, obs.C, 1)

	hub.SetDuplicate(1.0) // 通知被复制投递
	srv.Notify("/res", []byte("n1"))

	ns := recvN(t, obs.C, 1)
	if string(ns[0].Msg.Payload) != "n1" {
		t.Fatalf("unexpected: %q", ns[0].Msg.Payload)
	}
	// 重复副本不得再次投递。
	select {
	case nt := <-obs.C:
		t.Fatalf("duplicate notification delivered: %q", nt.Msg.Payload)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestObserveUnsubscribeNotObservable(t *testing.T) {
	// 服务器不支持观察（普通处理器不带 Observe 选项响应时，
	// 本实现的服务器总是附加 Observe，故用手工服务器模拟）。
	hub := NewMemHub()
	srvConn := hub.Endpoint("srv")
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
			// 响应不带 Observe 选项。
			ack := &Message{Type: ACK, Code: Content, MessageID: msg.MessageID, Token: msg.Token, Payload: []byte("plain")}
			enc, _ := ack.Encode()
			_ = srvConn.WriteTo(enc, from)
		}
	}()
	cliConn := hub.Endpoint("cli")
	cli := NewClient(cliConn, ClientConfig{AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1.0})
	defer cli.Close()

	_, err := cli.Observe(context.Background(), "srv", "/plain")
	if err != ErrNotObservable {
		t.Fatalf("want ErrNotObservable, got %v", err)
	}
}

// TestObserveRSTUnsubscribes 验证对通知回 RST 后服务器注销订阅。
func TestObserveRSTUnsubscribes(t *testing.T) {
	hub := NewMemHub()
	cli, srv := newPair(t, hub, HandlerFunc(observableHandler))

	obs, err := cli.Observe(context.Background(), "srv", "/r")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	_ = recvN(t, obs.C, 1)

	// 嗅探通知 MID，并拦截客户端的自动 ACK（模拟订阅方消失：
	// 真实消失的订阅者不会先 ACK 再 RST）。
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
				return true // 丢弃自动 ACK
			}
		}
		return false
	})
	srv.Notify("/r", []byte("x"))
	_ = recvN(t, obs.C, 1)
	mid := <-notifMID

	// 客户端对通知回 RST（模拟订阅方消失）。
	cliConn := hub.Endpoint("cli")
	rst := &Message{Type: RST, Code: CodeEmpty, MessageID: mid}
	enc, _ := rst.Encode()
	_ = cliConn.WriteTo(enc, "srv")

	deadline := time.Now().Add(time.Second)
	for len(srv.obs.forPath("/r")) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(srv.obs.forPath("/r")); got != 0 {
		t.Fatalf("subscription not removed after RST, %d left", got)
	}
	obs.Cancel(context.Background())
}
