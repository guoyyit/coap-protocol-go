package coap

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"
)

// newBlockwisePair 创建小报文上限的客户端/服务器对，强制走块传输。
func newBlockwisePair(t *testing.T, hub *MemHub, handler Handler) (*Client, *Server) {
	t.Helper()
	if hub == nil {
		hub = NewMemHub()
	}
	srv := NewServer(hub.Endpoint("srv"), ServerConfig{MaxMessageSize: 200}, handler)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	cli := NewClient(hub.Endpoint("cli"), ClientConfig{
		AckTimeout:      50 * time.Millisecond,
		AckRandomFactor: 1.0,
		MaxMessageSize:  200,
	})
	t.Cleanup(func() {
		cli.Close()
		srv.Shutdown(context.Background())
	})
	return cli, srv
}

func TestBlock1Upload(t *testing.T) {
	payload := make([]byte, 3000)
	_, _ = rand.Read(payload)

	var got []byte
	cli, _ := newBlockwisePair(t, nil, HandlerFunc(func(w ResponseWriter, r *Request) {
		got = append([]byte(nil), r.Msg.Payload...)
		_ = w.Write(&Message{Code: Changed})
	}))

	resp, err := cli.Do(context.Background(), "srv", &Message{Code: PUT, Payload: payload})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Msg.Code != Changed {
		t.Fatalf("unexpected code: %v", resp.Msg.Code)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reassembled payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestBlock2Download(t *testing.T) {
	payload := make([]byte, 5000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	cli, _ := newBlockwisePair(t, nil, HandlerFunc(func(w ResponseWriter, r *Request) {
		_ = w.Write(&Message{Code: Content, Payload: payload})
	}))

	resp, err := cli.Do(context.Background(), "srv", &Message{Code: GET})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !bytes.Equal(resp.Msg.Payload, payload) {
		t.Fatalf("block2 reassembly mismatch: got %d bytes", len(resp.Msg.Payload))
	}
}

func TestBlockwiseUnderLoss(t *testing.T) {
	hub := NewMemHub()
	// 确定性丢包：每第 5 个报文丢弃，触发重传但传输最终完成。
	var n int
	hub.SetFilter(func(from, to Addr, data []byte) bool {
		n++
		return n%5 == 0
	})
	payload := bytes.Repeat([]byte("ab"), 1000) // 2000 字节
	cli, _ := newBlockwisePair(t, hub, HandlerFunc(func(w ResponseWriter, r *Request) {
		_ = w.Write(&Message{Code: Content, Payload: payload})
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, "srv", &Message{Code: GET})
	if err != nil {
		t.Fatalf("Do under loss: %v", err)
	}
	if !bytes.Equal(resp.Msg.Payload, payload) {
		t.Fatalf("payload mismatch under loss: got %d bytes", len(resp.Msg.Payload))
	}
}

func TestBlockAssemblerOutOfOrder(t *testing.T) {
	// 构造 3 块数据，乱序喂入。
	full := []byte("0123456789ABCDEF0123456789ABCDEF0123") // 36 字节
	bs := 16
	blocks := []Block{
		{Num: 0, More: true, Size: bs},
		{Num: 1, More: true, Size: bs},
		{Num: 2, More: false, Size: bs},
	}
	chunks := [][]byte{full[0:16], full[16:32], full[32:36]}

	asm := newBlockAssembler(bs)
	// 乱序：2, 0, 1。
	for _, i := range []int{2, 0, 1} {
		got, done, err := asm.add(blocks[i], chunks[i])
		if err != nil {
			t.Fatalf("add block %d: %v", i, err)
		}
		if i != 1 && done {
			t.Fatalf("premature done at block %d", i)
		}
		if i == 1 {
			if !done {
				t.Fatal("not done after all blocks")
			}
			if !bytes.Equal(got, full) {
				t.Fatalf("reassembly mismatch: %q", got)
			}
		}
	}
}

func TestBlockAssemblerDuplicate(t *testing.T) {
	asm := newBlockAssembler(16)
	b0 := Block{Num: 0, More: true, Size: 16}
	b1 := Block{Num: 1, More: false, Size: 16}
	c0 := bytes.Repeat([]byte("x"), 16)
	c1 := []byte("end")

	if _, _, err := asm.add(b0, c0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := asm.add(b0, c0); err != nil { // 重复块：幂等
		t.Fatal(err)
	}
	got, done, err := asm.add(b1, c1)
	if err != nil || !done {
		t.Fatalf("final: %v %v", done, err)
	}
	if !bytes.Equal(got, append(c0, c1...)) {
		t.Fatalf("mismatch: %q", got)
	}
}

func TestBlockAssemblerConflict(t *testing.T) {
	asm := newBlockAssembler(16)
	b0 := Block{Num: 0, More: true, Size: 16}
	if _, _, err := asm.add(b0, bytes.Repeat([]byte("a"), 16)); err != nil {
		t.Fatal(err)
	}
	// 同号块内容冲突：必须报错，不得拼出错误内容。
	if _, _, err := asm.add(b0, bytes.Repeat([]byte("b"), 16)); !errors.Is(err, ErrBlockCorrupted) {
		t.Fatalf("want ErrBlockCorrupted, got %v", err)
	}
}

func TestBlockAssemblerTruncatedNonFinal(t *testing.T) {
	asm := newBlockAssembler(16)
	// 非末块未满载：违反 RFC 7959，拒绝。
	if _, _, err := asm.add(Block{Num: 0, More: true, Size: 16}, []byte("short")); !errors.Is(err, ErrBlockCorrupted) {
		t.Fatalf("want ErrBlockCorrupted, got %v", err)
	}
}

func TestBlockCodec(t *testing.T) {
	for _, b := range []Block{
		{Num: 0, More: false, Size: 16},
		{Num: 1, More: true, Size: 128},
		{Num: 300, More: true, Size: 1024},
		{Num: 0xFFFFF, More: false, Size: 512},
	} {
		enc, err := encodeBlock(b)
		if err != nil {
			t.Fatalf("encode %+v: %v", b, err)
		}
		got, err := decodeBlock(enc)
		if err != nil || got != b {
			t.Fatalf("roundtrip %+v -> %+v (%v)", b, got, err)
		}
	}
	if _, err := decodeBlock([]byte{1, 2, 3, 4}); !errors.Is(err, ErrBlockSize) {
		t.Fatalf("want ErrBlockSize, got %v", err)
	}
	if _, err := encodeBlock(Block{Num: 0, Size: 100}); !errors.Is(err, ErrBlockSize) {
		t.Fatalf("want ErrBlockSize, got %v", err)
	}
}

func TestBlock1MultipleClientsConcurrent(t *testing.T) {
	// 多客户端并发上传大负载，按（来源，令牌）隔离组装状态。
	hub := NewMemHub()
	srv := NewServer(hub.Endpoint("srv"), ServerConfig{MaxMessageSize: 200}, HandlerFunc(func(w ResponseWriter, r *Request) {
		_ = w.Write(&Message{Code: Changed, Payload: []byte(fmt.Sprintf("len=%d", len(r.Msg.Payload)))})
	}))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	const N = 4
	errs := make(chan error, N)
	for c := 0; c < N; c++ {
		go func(c int) {
			cli := NewClient(hub.Endpoint(fmt.Sprintf("cli-%d", c)), ClientConfig{
				AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1.0, MaxMessageSize: 200,
			})
			defer cli.Close()
			payload := bytes.Repeat([]byte{byte(c)}, 1500)
			resp, err := cli.Do(context.Background(), "srv", &Message{Code: PUT, Payload: payload})
			if err != nil {
				errs <- err
				return
			}
			if string(resp.Msg.Payload) != "len=1500" {
				errs <- fmt.Errorf("client %d: bad resp %q", c, resp.Msg.Payload)
				return
			}
			errs <- nil
		}(c)
	}
	for i := 0; i < N; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
