package coap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestDuplicateRequestHandledOnce 验证同一（来源，消息编号）的 CON
// 被重复投递时，处理器只执行一次，且重复方仍能收到缓存响应。
func TestDuplicateRequestHandledOnce(t *testing.T) {
	hub := NewMemHub()
	hub.SetDuplicate(1.0) // 每个报文都被复制投递一次
	var calls atomic.Int32
	cli, _ := newPair(t, hub, HandlerFunc(func(w ResponseWriter, r *Request) {
		calls.Add(1)
		_ = w.Write(&Message{Code: Content, Payload: []byte("done")})
	}))

	resp, err := cli.Do(context.Background(), "srv", &Message{Code: POST, Payload: []byte("a")})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Msg.Payload) != "done" {
		t.Fatalf("unexpected payload: %q", resp.Msg.Payload)
	}
	// 等待可能的延迟重复投递。
	time.Sleep(50 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("handler called %d times, want 1", n)
	}
}

// TestRetransmittedRequestIdempotent 验证 ACK 丢失导致客户端重传时，
// 服务器侧处理仍然幂等，且重传的请求能拿到缓存的响应。
func TestRetransmittedRequestIdempotent(t *testing.T) {
	hub := NewMemHub()
	var ackDropped atomic.Int32
	// 丢弃服务器发出的首个 ACK，迫使客户端重传同一 MID。
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "srv" && to == "cli" {
			return ackDropped.Add(1) <= 1
		}
		return false
	})
	var calls atomic.Int32
	cli, _ := newPair(t, hub, HandlerFunc(func(w ResponseWriter, r *Request) {
		calls.Add(1)
		_ = w.Write(&Message{Code: Changed, Payload: []byte("v1")})
	}))

	resp, err := cli.Do(context.Background(), "srv", &Message{Code: PUT, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Msg.Code != Changed || string(resp.Msg.Payload) != "v1" {
		t.Fatalf("unexpected response: %+v", resp.Msg)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("handler called %d times, want 1 (idempotent)", n)
	}
}

// TestDuplicateACKIgnored 验证重复 ACK 不会使客户端状态机错乱。
func TestDuplicateACKIgnored(t *testing.T) {
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
			ack := &Message{Type: ACK, Code: Content, MessageID: msg.MessageID, Token: msg.Token, Payload: []byte("r")}
			enc, _ := ack.Encode()
			_ = srvConn.WriteTo(enc, from)
			_ = srvConn.WriteTo(enc, from) // 重复 ACK
			_ = srvConn.WriteTo(enc, from)
		}
	}()
	cliConn := hub.Endpoint("cli")
	cli := NewClient(cliConn, ClientConfig{AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1.0})
	defer cli.Close()

	resp, err := cli.Do(context.Background(), "srv", &Message{Code: GET})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Msg.Payload) != "r" {
		t.Fatalf("unexpected payload: %q", resp.Msg.Payload)
	}
	// 后续请求不受残留状态影响。
	resp2, err := cli.Do(context.Background(), "srv", &Message{Code: GET})
	if err != nil || string(resp2.Msg.Payload) != "r" {
		t.Fatalf("second Do failed: %v %v", resp2, err)
	}
	cli.mu.Lock()
	n := len(cli.pending)
	cli.mu.Unlock()
	if n != 0 {
		t.Fatalf("dangling pending: %d", n)
	}
}

// TestOutOfOrderACKs 验证多个在途请求的 ACK 乱序返回时各自正确匹配。
func TestOutOfOrderACKs(t *testing.T) {
	hub := NewMemHub()
	srvConn := hub.Endpoint("srv")
	go func() {
		type item struct {
			from Addr
			msg  *Message
		}
		var held []item
		for {
			data, from, err := srvConn.ReadFrom()
			if err != nil {
				return
			}
			msg, err := Decode(data)
			if err != nil || msg.Type != CON {
				continue
			}
			held = append(held, item{from, msg})
			if len(held) == 3 {
				// 逆序回复，制造乱序 ACK。
				for i := len(held) - 1; i >= 0; i-- {
					it := held[i]
					ack := &Message{Type: ACK, Code: Content, MessageID: it.msg.MessageID,
						Token: it.msg.Token, Payload: it.msg.Payload}
					enc, _ := ack.Encode()
					_ = srvConn.WriteTo(enc, it.from)
				}
				held = nil
			}
		}
	}()
	cliConn := hub.Endpoint("cli")
	cli := NewClient(cliConn, ClientConfig{AckTimeout: time.Second, AckRandomFactor: 1.0})
	defer cli.Close()

	results := make(chan string, 3)
	for i, p := range []string{"A", "B", "C"} {
		_ = i
		go func(payload string) {
			resp, err := cli.Do(context.Background(), "srv", &Message{Code: POST, Payload: []byte(payload)})
			if err != nil {
				results <- "ERR:" + payload
				return
			}
			results <- string(resp.Msg.Payload)
		}(p)
	}
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case r := <-results:
			got[r] = true
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for responses")
		}
	}
	if !got["A"] || !got["B"] || !got["C"] {
		t.Fatalf("responses mismatched: %v", got)
	}
}
