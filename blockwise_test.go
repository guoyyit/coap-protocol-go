package coap

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"
)

func TestBlockOptionRoundTrip(t *testing.T) {
	bo := BlockOption{Num: 0xABCDE, M: true, SZX: Block1024}
	got := DecodeBlock(bo.Encode())
	if got != bo {
		t.Fatalf("got %+v, want %+v", got, bo)
	}
	if bo.Size() != 1024 {
		t.Fatalf("size = %d", bo.Size())
	}
}

func TestReassemblerOutOfOrder(t *testing.T) {
	payload := make([]byte, 100) // 4 blocks of 32: 32+32+32+4
	for i := range payload {
		payload[i] = byte(i)
	}
	blocks := splitBlocks(payload, Block32)
	if len(blocks) != 4 {
		t.Fatalf("blocks = %d, want 4", len(blocks))
	}
	r := newBlockReassembler(Block32)
	// Feed in scrambled order: 2, 0, 3, 1.
	for _, i := range []int{2, 0, 3, 1} {
		full, complete, err := r.Add(blocks[i], blockSlice(payload, blocks[i]))
		if err != nil {
			t.Fatalf("Add block %d: %v", i, err)
		}
		if i != 1 && complete {
			t.Fatalf("complete too early at block %d", i)
		}
		if i == 1 {
			if !complete {
				t.Fatal("not complete after all blocks")
			}
			if !bytes.Equal(full, payload) {
				t.Fatal("reassembled payload mismatch")
			}
		}
	}
}

func TestReassemblerDuplicateIgnored(t *testing.T) {
	payload := []byte("01234567890123456789012345678901234567890123456789") // 50 bytes
	blocks := splitBlocks(payload, Block16)
	r := newBlockReassembler(Block16)
	for _, b := range blocks {
		// Deliver every block twice.
		for k := 0; k < 2; k++ {
			full, complete, err := r.Add(b, blockSlice(payload, b))
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			if complete {
				if !bytes.Equal(full, payload) {
					t.Fatal("payload mismatch with duplicates")
				}
				return
			}
		}
	}
	t.Fatal("never completed")
}

func TestReassemblerRejectsInconsistent(t *testing.T) {
	r := newBlockReassembler(Block16)
	// Non-final block with wrong size must abort.
	_, _, err := r.Add(BlockOption{Num: 0, M: true, SZX: Block16}, []byte("short"))
	if err == nil {
		t.Fatal("expected error for undersized non-final block")
	}

	r2 := newBlockReassembler(Block16)
	if _, _, err := r2.Add(BlockOption{Num: 0, M: true, SZX: Block16}, make([]byte, 16)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// SZX change mid-transfer must abort.
	if _, _, err := r2.Add(BlockOption{Num: 1, M: true, SZX: Block32}, make([]byte, 32)); err == nil {
		t.Fatal("expected error for SZX change")
	}
}

func TestGetLargeBlock2(t *testing.T) {
	payload := make([]byte, 300)
	_, _ = rand.Read(payload)
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	srv.Handle("/big", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: payload}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	got, err := cli.GetLarge(context.Background(), "/big", "server", Block32)
	if err != nil {
		t.Fatalf("GetLarge: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, want %d, content mismatch", len(got), len(payload))
	}
}

func TestPutLargeBlock1(t *testing.T) {
	payload := make([]byte, 200)
	_, _ = rand.Read(payload)
	var received []byte
	ct, st := NewPipe("client", "server", PipeConfig{})
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	srv.Handle("/upload", func(req *Message, addr string) (*Message, error) {
		received = append([]byte(nil), req.Payload...)
		return &Message{Code: CodeChanged}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	if _, err := cli.PutLarge(context.Background(), "/upload", payload, "server", Block32); err != nil {
		t.Fatalf("PutLarge: %v", err)
	}
	// Give the final block time to be processed.
	deadline := time.Now().Add(time.Second)
	for !bytes.Equal(received, payload) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("server got %d bytes, want %d", len(received), len(payload))
	}
}

func TestGetLargeWithLossAndRetransmit(t *testing.T) {
	payload := make([]byte, 128)
	_, _ = rand.Read(payload)
	var clientMsgs int
	cfg := PipeConfig{
		Drop: func(data []byte, from, to string) bool {
			if from != "client" {
				return false
			}
			clientMsgs++
			return clientMsgs == 2 // lose one block request once
		},
	}
	ct, st := NewPipe("client", "server", cfg)
	srv := NewServer(st, DefaultServerConfig(), "")
	defer srv.Close()
	srv.Handle("/big", func(req *Message, addr string) (*Message, error) {
		return &Message{Code: CodeContent, Payload: payload}, nil
	})
	cli := NewClient(ct, fastConfig())
	defer cli.Close()

	got, err := cli.GetLarge(context.Background(), "/big", "server", Block32)
	if err != nil {
		t.Fatalf("GetLarge: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch after lossy transfer")
	}
}
