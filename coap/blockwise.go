package coap

import (
	"context"
	"errors"
	"fmt"
)

// 块传输相关错误。
var (
	ErrBlockSize      = errors.New("coap: invalid block size exponent")
	ErrBlockCorrupted = errors.New("coap: block assembly inconsistent")
	ErrBlockTooLarge  = errors.New("coap: peer requested entity too large")
)

// Block 描述 Block1/Block2 选项值：编号、是否有后续块、块大小。
type Block struct {
	Num  uint32 // 块序号，从 0 开始
	More bool   // M 位：是否还有后续块
	Size int    // 块字节数，必须为 16/32/.../1024
}

// szxOf 返回块大小对应的 SZX 指数。
func szxOf(size int) (int, error) {
	switch size {
	case 16:
		return 0, nil
	case 32:
		return 1, nil
	case 64:
		return 2, nil
	case 128:
		return 3, nil
	case 256:
		return 4, nil
	case 512:
		return 5, nil
	case 1024:
		return 6, nil
	default:
		return 0, ErrBlockSize
	}
}

// encodeBlock 将 Block 编码为选项字节。
func encodeBlock(b Block) ([]byte, error) {
	szx, err := szxOf(b.Size)
	if err != nil {
		return nil, err
	}
	if b.Num > 0xFFFFF {
		return nil, ErrBlockSize
	}
	v := b.Num<<4 | uint32(szx)
	if b.More {
		v |= 0x8
	}
	return encodeUint(v), nil
}

// decodeBlock 从选项字节解析 Block。
func decodeBlock(v []byte) (Block, error) {
	if len(v) > 3 {
		return Block{}, ErrBlockSize
	}
	n, err := decodeUint(v)
	if err != nil {
		return Block{}, err
	}
	szx := n & 0x7
	if szx == 7 {
		return Block{}, ErrBlockSize
	}
	return Block{
		Num:  n >> 4,
		More: n&0x8 != 0,
		Size: 16 << szx,
	}, nil
}

// blockOption 从选项集中读取并解析块选项。
func blockOption(o Options, id OptionID) (Block, bool) {
	v, ok := o.Get(id)
	if !ok {
		return Block{}, false
	}
	b, err := decodeBlock(v)
	if err != nil {
		return Block{}, false
	}
	return b, true
}

// setBlock 将块选项写入选项集。
func setBlock(o *Options, id OptionID, b Block) {
	if v, err := encodeBlock(b); err == nil {
		o.Set(id, v)
	}
}

// blockAssembler 在接收侧按序重组分块负载，
// 容忍乱序与重复，块内容冲突时报错。
type blockAssembler struct {
	size     int
	blocks   map[uint32][]byte
	lastNum  uint32
	hasLast  bool
	total    int
	received int
}

func newBlockAssembler(size int) *blockAssembler {
	return &blockAssembler{size: size, blocks: make(map[uint32][]byte)}
}

// add 接收一个块；完成时返回完整负载与 true。
// 乱序块被缓存；重复块内容一致时幂等忽略，冲突时报错。
func (a *blockAssembler) add(b Block, payload []byte) (full []byte, done bool, err error) {
	if b.Size != a.size {
		return nil, false, ErrBlockCorrupted
	}
	if len(payload) > b.Size {
		return nil, false, ErrBlockCorrupted
	}
	if b.More && len(payload) != b.Size {
		return nil, false, ErrBlockCorrupted // 非末块必须满载
	}
	if !b.More {
		if a.hasLast && a.lastNum != b.Num {
			return nil, false, ErrBlockCorrupted // 末块编号冲突
		}
		a.hasLast = true
		a.lastNum = b.Num
		a.total = int(b.Num)*b.Size + len(payload)
	}
	if old, ok := a.blocks[b.Num]; ok {
		if string(old) != string(payload) {
			return nil, false, ErrBlockCorrupted // 同号块内容冲突
		}
		// 重复块：幂等忽略
	} else {
		a.blocks[b.Num] = append([]byte(nil), payload...)
		a.received += len(payload)
	}
	if !a.hasLast {
		return nil, false, nil
	}
	for i := uint32(0); i <= a.lastNum; i++ {
		if _, ok := a.blocks[i]; !ok {
			return nil, false, nil
		}
	}
	out := make([]byte, 0, a.total)
	for i := uint32(0); i <= a.lastNum; i++ {
		out = append(out, a.blocks[i]...)
	}
	if len(out) != a.total {
		return nil, false, ErrBlockCorrupted
	}
	return out, true, nil
}

// ---------------- 客户端块传输 ----------------

// blockSizeFor 选择不超过限制的块大小。
func blockSizeFor(limit int) int {
	size := 1024
	for size > limit && size > 16 {
		size /= 2
	}
	return size
}

// doBlock1 将超大请求负载分块上传（Block1）。
func (c *Client) doBlock1(ctx context.Context, to Addr, req *Message) (*Response, error) {
	bs := blockSizeFor(c.cfg.MaxMessageSize - 64)
	if bs < 16 {
		return nil, ErrMessageTooLarge
	}
	payload := req.Payload
	total := len(payload)
	var lastResp *Message
	for off := 0; off < total || (total == 0 && off == 0); off += bs {
		end := off + bs
		if end > total {
			end = total
		}
		chunk := payload[off:end]
		m := &Message{
			Type:    CON,
			Code:    req.Code,
			Token:   req.Token,
			Options: append(Options(nil), req.Options...),
			Payload: chunk,
		}
		setBlock(&m.Options, OptionBlock1, Block{
			Num:  uint32(off / bs),
			More: end < total,
			Size: bs,
		})
		m.Options.SetUint(OptionSize1, uint32(total))
		resp, err := c.roundTrip(ctx, to, m)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, ErrTimeout
		}
		if end < total {
			// 中间块：期望 2.31 Continue。
			if resp.Code != Continue {
				return nil, fmt.Errorf("coap: block1 transfer rejected: code %d", resp.Code)
			}
		} else {
			lastResp = resp
		}
		if total == 0 {
			break
		}
	}
	return &Response{Msg: lastResp}, nil
}

// fetchBlock2 在首个响应块之后继续拉取并重组完整响应（Block2）。
func (c *Client) fetchBlock2(ctx context.Context, to Addr, req, firstResp *Message, first Block) (*Message, error) {
	asm := newBlockAssembler(first.Size)
	if _, _, err := asm.add(first, firstResp.Payload); err != nil {
		return nil, err
	}
	result := firstResp.Clone()
	next := first
	for next.More {
		follow := &Message{
			Type:    CON,
			Code:    req.Code,
			Token:   req.Token,
			Options: append(Options(nil), req.Options...),
		}
		follow.Options.Remove(OptionBlock1)
		follow.Options.Remove(OptionSize1)
		follow.Payload = nil
		setBlock(&follow.Options, OptionBlock2, Block{Num: next.Num + 1, Size: next.Size})
		resp, err := c.roundTrip(ctx, to, follow)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, ErrTimeout
		}
		b, ok := blockOption(resp.Options, OptionBlock2)
		if !ok || b.Num != next.Num+1 {
			return nil, ErrBlockCorrupted // 响应块编号与请求不符
		}
		full, done, err := asm.add(b, resp.Payload)
		if err != nil {
			return nil, err
		}
		if done {
			result.Options.Remove(OptionBlock2)
			result.Options = mergeOptions(result.Options, resp.Options, OptionBlock2)
			result.Code = resp.Code
			result.Payload = full
			return result, nil
		}
		next = b
	}
	return result, nil
}

// mergeOptions 合并选项：以 base 为准，追加 other 中除 exclude 外的选项。
func mergeOptions(base, other Options, exclude OptionID) Options {
	out := append(Options(nil), base...)
	for _, o := range other {
		if o.ID == exclude {
			continue
		}
		out.Set(o.ID, o.Value)
	}
	return out
}
