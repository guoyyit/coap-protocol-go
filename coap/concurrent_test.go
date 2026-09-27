package coap

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentRequests 验证大量并发在途请求各自正确匹配响应。
func TestConcurrentRequests(t *testing.T) {
	hub := NewMemHub()
	hub.SetReorder(0.3) // 注入乱序
	cli, _ := newPair(t, hub, HandlerFunc(func(w ResponseWriter, r *Request) {
		// 随机微小延迟放大乱序。
		time.Sleep(time.Duration(len(r.Msg.Payload)%7) * time.Millisecond)
		_ = w.Write(&Message{Code: Content, Payload: append([]byte("ack:"), r.Msg.Payload...)})
	}))

	const N = 32
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := fmt.Sprintf("req-%d", i)
			resp, err := cli.Do(context.Background(), "srv", &Message{Code: POST, Payload: []byte(p)})
			if err != nil {
				errs <- fmt.Errorf("req %d: %w", i, err)
				return
			}
			if string(resp.Msg.Payload) != "ack:"+p {
				errs <- fmt.Errorf("req %d: mismatched payload %q", i, resp.Msg.Payload)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestTokenReuseStaleResponse 验证令牌复用后，迟到的旧响应
// （旧消息编号）不会被误配给复用同一令牌的新请求。
func TestTokenReuseStaleResponse(t *testing.T) {
	hub := NewMemHub()

	// 嗅探客户端发出的请求，按令牌记录消息编号。
	type midRecord struct{ mid uint16 }
	midCh := make(chan midRecord, 16)
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		if from == "cli" && to == "srv" {
			if m, err := Decode(data); err == nil && m.Code.IsRequest() {
				midCh <- midRecord{mid: m.MessageID}
			}
		}
		return false
	})

	cli, _ := newPair(t, hub, HandlerFunc(func(w ResponseWriter, r *Request) {
		switch string(r.Msg.Payload) {
		case "first":
			_ = w.Write(&Message{Code: Content, Payload: []byte("resp-first")})
		case "second":
			time.Sleep(150 * time.Millisecond) // 留出注入陈旧响应的窗口
			_ = w.Write(&Message{Code: Content, Payload: []byte("resp-second")})
		}
	}))

	token := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	// 第一个请求：正常完成。
	req1 := &Message{Code: POST, Token: token, Payload: []byte("first")}
	resp1, err := cli.Do(context.Background(), "srv", req1)
	if err != nil || string(resp1.Msg.Payload) != "resp-first" {
		t.Fatalf("first request failed: %v %v", resp1, err)
	}
	rec1 := <-midCh // 记录旧 MID

	// 复用同一令牌发起第二个请求。
	req2 := &Message{Code: POST, Token: token, Payload: []byte("second")}
	resultCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := cli.Do(context.Background(), "srv", req2)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- string(resp.Msg.Payload)
	}()
	rec2 := <-midCh // 新 MID
	if rec2.mid == rec1.mid {
		t.Fatal("MID not changed between requests")
	}

	// 从服务器地址注入迟到的旧响应：旧 MID + 复用的令牌。
	srvConn := hub.Endpoint("srv")
	stale := &Message{Type: ACK, Code: Content, MessageID: rec1.mid, Token: token, Payload: []byte("STALE")}
	enc, _ := stale.Encode()
	if err := srvConn.WriteTo(enc, "cli"); err != nil {
		t.Fatalf("inject stale: %v", err)
	}

	select {
	case got := <-resultCh:
		if got != "resp-second" {
			t.Fatalf("stale response misdelivered: got %q", got)
		}
	case err := <-errCh:
		t.Fatalf("second request failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("second request timed out")
	}
}

// TestLateResponseAfterCompletion 验证请求完成后迟到的响应被静默丢弃，
// 不影响后续交换。
func TestLateResponseAfterCompletion(t *testing.T) {
	hub := NewMemHub()
	cli, _ := newPair(t, hub, HandlerFunc(echoHandler))

	resp, err := cli.Do(context.Background(), "srv", &Message{Code: POST, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_ = resp

	// 注入一个与任何在途请求都不匹配的响应。
	srvConn := hub.Endpoint("srv")
	ghost := &Message{Type: ACK, Code: Content, MessageID: 0x7777, Token: []byte{1, 2, 3, 4}, Payload: []byte("ghost")}
	enc, _ := ghost.Encode()
	_ = srvConn.WriteTo(enc, "cli")

	// 后续请求不受影响。
	resp2, err := cli.Do(context.Background(), "srv", &Message{Code: POST, Payload: []byte("y")})
	if err != nil || string(resp2.Msg.Payload) != "echo:y" {
		t.Fatalf("subsequent request broken: %v %v", resp2, err)
	}
}
