package coap

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// BlockSize is the encoded SZX size exponent (block bytes = 1<<(szx+4)).
const (
	Block16   = 0
	Block32   = 1
	Block64   = 2
	Block128  = 3
	Block256  = 4
	Block512  = 5
	Block1024 = 6
)

// MaxBlockSZX caps supported block size at 1024 bytes.
const MaxBlockSZX = Block1024

// BlockOption encodes/decodes Block1/Block2 option values (RFC 7959).
type BlockOption struct {
	Num uint32 // block number
	M   bool   // more blocks follow
	SZX uint8  // size exponent
}

// Encode packs the block option into its uint wire value.
func (b BlockOption) Encode() uint32 {
	v := b.Num << 4
	if b.M {
		v |= 1 << 3
	}
	return v | uint32(b.SZX&0x7)
}

// DecodeBlock parses a uint wire value into a BlockOption.
func DecodeBlock(v uint32) BlockOption {
	return BlockOption{
		Num: v >> 4,
		M:   v&0x8 != 0,
		SZX: uint8(v & 0x7),
	}
}

// Size returns the block byte size.
func (b BlockOption) Size() int { return 1 << (uint(b.SZX) + 4) }

// blockReassembler collects out-of-order blocks for one transfer.
// It never emits a payload until every block 0..total-1 arrived exactly
// once with consistent sizes, so loss, duplication and reordering cannot
// produce corrupt output.
type blockReassembler struct {
	mu        sync.Mutex
	blocks    map[uint32][]byte
	total     int // known when M=false seen, else -1
	szx       uint8
	updatedAt time.Time
}

func newBlockReassembler(szx uint8) *blockReassembler {
	return &blockReassembler{
		blocks: make(map[uint32][]byte),
		total:  -1,
		szx:    szx,
	}
}

// Add ingests one block. Returns (payload, complete, err). Duplicate blocks
// are ignored; conflicting sizes or block numbers past the announced total
// abort the transfer with an error.
func (r *blockReassembler) Add(b BlockOption, data []byte) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updatedAt = time.Now()

	if b.SZX != r.szx {
		return nil, false, fmt.Errorf("%w: SZX changed mid-transfer", ErrBlockTransfer)
	}
	size := b.Size()
	// All blocks except the final one must be exactly `size` bytes.
	if b.M && len(data) != size {
		return nil, false, fmt.Errorf("%w: non-final block has %d bytes, want %d",
			ErrBlockTransfer, len(data), size)
	}
	if !b.M {
		if len(data) == 0 || len(data) > size {
			return nil, false, fmt.Errorf("%w: final block has %d bytes", ErrBlockTransfer, len(data))
		}
		if r.total >= 0 && int(b.Num) != r.total-1 {
			return nil, false, fmt.Errorf("%w: conflicting final block", ErrBlockTransfer)
		}
		r.total = int(b.Num) + 1
	}
	if r.total >= 0 && int(b.Num) >= r.total {
		return nil, false, fmt.Errorf("%w: block %d past total %d", ErrBlockTransfer, b.Num, r.total)
	}
	if _, dup := r.blocks[b.Num]; dup {
		return nil, false, nil // duplicate: ignore, keep waiting
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	r.blocks[b.Num] = cp

	if r.total < 0 || len(r.blocks) < r.total {
		return nil, false, nil
	}
	// All blocks present: concatenate in order.
	out := make([]byte, 0, r.total*size)
	for i := 0; i < r.total; i++ {
		blk, ok := r.blocks[uint32(i)]
		if !ok {
			return nil, false, nil // gap (shouldn't happen given len check)
		}
		out = append(out, blk...)
	}
	return out, true, nil
}

// splitBlocks returns the block descriptors for payload at the given SZX.
// Block i carries payload[i*size : min((i+1)*size, len)].
func splitBlocks(payload []byte, szx uint8) []BlockOption {
	size := 1 << (uint(szx) + 4)
	if len(payload) == 0 {
		return []BlockOption{{Num: 0, M: false, SZX: szx}}
	}
	n := (len(payload) + size - 1) / size
	out := make([]BlockOption, n)
	for i := 0; i < n; i++ {
		out[i] = BlockOption{Num: uint32(i), M: i < n-1, SZX: szx}
	}
	return out
}

// blockSlice returns the payload bytes carried by block b.
func blockSlice(payload []byte, b BlockOption) []byte {
	size := b.Size()
	start := int(b.Num) * size
	if start >= len(payload) {
		return nil
	}
	end := start + size
	if end > len(payload) {
		end = len(payload)
	}
	return payload[start:end]
}

// --- client-side block-wise helpers ---

// GetLarge fetches a resource that may exceed one datagram, requesting
// Block2 chunks of the given size and reassembling them in order.
func (c *Client) GetLarge(ctx context.Context, path, addr string, szx uint8) ([]byte, error) {
	r := newBlockReassembler(szx)
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	for num := uint32(0); ; num++ {
		req := &Message{Code: CodeGET, Token: token}
		req.SetPath(path)
		req.SetUint(OptionBlock2, BlockOption{Num: num, SZX: szx}.Encode())
		resp, err := c.Do(ctx, req, addr)
		if err != nil {
			return nil, err
		}
		bv, ok := resp.GetUint(OptionBlock2)
		if !ok {
			// Server answered without block-wise: payload fits in one.
			if num == 0 {
				return resp.Payload, nil
			}
			return nil, ErrBlockTransfer
		}
		bo := DecodeBlock(bv)
		if bo.Num != num {
			return nil, ErrBlockTransfer // out-of-order block response
		}
		full, complete, err := r.Add(bo, resp.Payload)
		if err != nil {
			return nil, err
		}
		if complete {
			return full, nil
		}
	}
}

// PutLarge uploads a payload that may exceed one datagram as Block1
// chunks of the given size, waiting for each acknowledgement.
func (c *Client) PutLarge(ctx context.Context, path string, payload []byte, addr string, szx uint8) (*Message, error) {
	blocks := splitBlocks(payload, szx)
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	var last *Message
	for _, desc := range blocks {
		req := &Message{Code: CodePUT, Token: token, Payload: blockSlice(payload, desc)}
		req.SetPath(path)
		req.SetUint(OptionBlock1, desc.Encode())
		resp, err := c.Do(ctx, req, addr)
		if err != nil {
			return nil, err
		}
		if resp.Code >= 128 { // 4.xx/5.xx
			return nil, ErrBlockTransfer
		}
		last = resp
	}
	return last, nil
}
