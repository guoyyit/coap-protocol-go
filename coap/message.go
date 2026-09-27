package coap

import (
	"encoding/binary"
	"errors"
	"sort"
)

// Type 为 CoAP 报文类型（RFC 7252 §3）。
type Type uint8

const (
	CON Type = iota // 可确认报文，需要对方 ACK
	NON             // 非确认报文
	ACK             // 确认报文
	RST             // 复位报文
)

// Code 为 CoAP 报文码，高 3 位为 class，低 5 位为 detail。
type Code uint8

const (
	CodeEmpty Code = 0 // 0.00 空报文

	// 请求码
	GET    Code = 1 // 0.01
	POST   Code = 2 // 0.02
	PUT    Code = 3 // 0.03
	DELETE Code = 4 // 0.04

	// 成功响应码 2.xx
	Created  Code = 65 // 2.01
	Deleted  Code = 66 // 2.02
	Valid    Code = 67 // 2.03
	Changed  Code = 68 // 2.04
	Content  Code = 69 // 2.05
	Continue Code = 95 // 2.31（块传输中间响应）

	// 客户端错误 4.xx
	BadRequest              Code = 128 // 4.00
	NotFound                Code = 132 // 4.04
	RequestEntityTooLarge   Code = 141 // 4.13
	RequestEntityIncomplete Code = 136 // 4.08

	// 服务器错误 5.xx
	InternalServerError Code = 160 // 5.00
)

// IsRequest 报告码是否为请求码。
func (c Code) IsRequest() bool { return c >= 1 && c <= 31 }

// IsResponse 报告码是否为响应码。
func (c Code) IsResponse() bool { return c >= 64 && c <= 191 }

// 报文编解码与校验相关错误。
var (
	ErrMessageTooShort  = errors.New("coap: message too short")
	ErrInvalidVersion   = errors.New("coap: unsupported version")
	ErrInvalidTokenLen  = errors.New("coap: invalid token length")
	ErrInvalidType      = errors.New("coap: invalid message type")
	ErrOptionsTruncated = errors.New("coap: options truncated")
	ErrPayloadMarker    = errors.New("coap: payload marker without payload")
	ErrOptionDelta      = errors.New("coap: option delta/order invalid")
	ErrMessageTooLarge  = errors.New("coap: message exceeds size limit")
	ErrEmptyHasBody     = errors.New("coap: empty message carries token/options/payload")
)

// MaxTokenLen 为 RFC 7252 规定的令牌最大长度。
const MaxTokenLen = 8

// protocolVersion 为本实现支持的 CoAP 版本。
const protocolVersion = 1

// payloadMarker 分隔选项区与负载区。
const payloadMarker = 0xFF

// Message 为一条 CoAP 报文。
type Message struct {
	Type      Type
	Code      Code
	MessageID uint16
	Token     []byte
	Options   Options
	Payload   []byte
}

// IsRequest 报告报文是否为请求。
func (m *Message) IsRequest() bool { return m.Code.IsRequest() }

// IsResponse 报告报文是否为响应。
func (m *Message) IsResponse() bool { return m.Code.IsResponse() }

// PathString 返回 URI-Path 拼接的路径。
func (m *Message) PathString() string { return m.Options.Path() }

// Clone 深拷贝报文，避免缓存与共享切片被意外修改。
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	c := *m
	if m.Token != nil {
		c.Token = append([]byte(nil), m.Token...)
	}
	if m.Options != nil {
		c.Options = make(Options, len(m.Options))
		for i, o := range m.Options {
			c.Options[i] = Option{ID: o.ID, Value: append([]byte(nil), o.Value...)}
		}
	}
	if m.Payload != nil {
		c.Payload = append([]byte(nil), m.Payload...)
	}
	return &c
}

// Encode 将报文序列化为数据报字节流。
// 选项在编码前按编号排序；令牌超长等非法输入返回错误。
func (m *Message) Encode() ([]byte, error) {
	if len(m.Token) > MaxTokenLen {
		return nil, ErrInvalidTokenLen
	}
	if m.Type > RST {
		return nil, ErrInvalidType
	}
	if m.Code == CodeEmpty && (len(m.Token) > 0 || len(m.Options) > 0 || len(m.Payload) > 0) {
		return nil, ErrEmptyHasBody
	}

	opts := make(Options, len(m.Options))
	copy(opts, m.Options)
	sort.Slice(opts, func(i, j int) bool { return opts[i].ID < opts[j].ID })

	buf := make([]byte, 0, 4+len(m.Token)+len(m.Payload)+32)
	hdr := byte(protocolVersion<<6) | byte(m.Type)<<4 | byte(len(m.Token))
	buf = append(buf, hdr, byte(m.Code), byte(m.MessageID>>8), byte(m.MessageID))
	buf = append(buf, m.Token...)

	var prev OptionID
	for _, o := range opts {
		delta := int(o.ID) - int(prev)
		dNib, dExt, err := encodeExtended(delta)
		if err != nil {
			return nil, err
		}
		lNib, lExt, err := encodeExtended(len(o.Value))
		if err != nil {
			return nil, err
		}
		buf = append(buf, dNib<<4|lNib)
		buf = append(buf, dExt...)
		buf = append(buf, lExt...)
		buf = append(buf, o.Value...)
		prev = o.ID
	}

	if len(m.Payload) > 0 {
		buf = append(buf, payloadMarker)
		buf = append(buf, m.Payload...)
	}
	return buf, nil
}

// Decode 解析数据报；任何畸形输入都返回错误而非 panic，
// 且不会修改调用方持有的数据。
func Decode(data []byte) (*Message, error) {
	if len(data) < 4 {
		return nil, ErrMessageTooShort
	}
	if ver := data[0] >> 6; ver != protocolVersion {
		return nil, ErrInvalidVersion
	}
	typ := Type((data[0] >> 4) & 0x3)
	tkl := int(data[0] & 0x0F)
	if tkl > MaxTokenLen {
		return nil, ErrInvalidTokenLen
	}
	code := Code(data[1])
	mid := binary.BigEndian.Uint16(data[2:4])

	rest := data[4:]
	if len(rest) < tkl {
		return nil, ErrMessageTooShort
	}
	var token []byte
	if tkl > 0 {
		token = append([]byte(nil), rest[:tkl]...)
		rest = rest[tkl:]
	}
	if code == CodeEmpty && (tkl != 0 || len(rest) != 0) {
		return nil, ErrEmptyHasBody
	}

	msg := &Message{Type: typ, Code: code, MessageID: mid, Token: token}

	var prev OptionID
	for len(rest) > 0 {
		if rest[0] == payloadMarker {
			rest = rest[1:]
			if len(rest) == 0 {
				return nil, ErrPayloadMarker
			}
			msg.Payload = rest
			rest = nil
			break
		}
		dNib, lNib := rest[0]>>4, rest[0]&0x0F
		rest = rest[1:]
		delta, err := decodeExtended(dNib, &rest)
		if err != nil {
			return nil, err
		}
		length, err := decodeExtended(lNib, &rest)
		if err != nil {
			return nil, err
		}
		if len(rest) < length {
			return nil, ErrOptionsTruncated
		}
		id := prev + OptionID(delta)
		val := rest[:length]
		rest = rest[length:]
		msg.Options = append(msg.Options, Option{ID: id, Value: append([]byte(nil), val...)})
		prev = id
	}
	return msg, nil
}

// encodeExtended 编码选项 delta/length 的扩展部分。
func encodeExtended(v int) (nibble byte, ext []byte, err error) {
	switch {
	case v < 0:
		return 0, nil, ErrOptionDelta
	case v < 13:
		return byte(v), nil, nil
	case v < 269:
		return 13, []byte{byte(v - 13)}, nil
	case v <= 65804:
		return 14, []byte{byte((v - 269) >> 8), byte(v - 269)}, nil
	default:
		return 0, nil, ErrOptionDelta
	}
}

// decodeExtended 解码选项 delta/length 的扩展部分，并推进 rest。
func decodeExtended(nibble byte, rest *[]byte) (int, error) {
	switch nibble {
	case 13:
		if len(*rest) < 1 {
			return 0, ErrOptionsTruncated
		}
		v := int((*rest)[0]) + 13
		*rest = (*rest)[1:]
		return v, nil
	case 14:
		if len(*rest) < 2 {
			return 0, ErrOptionsTruncated
		}
		v := int(binary.BigEndian.Uint16(*rest)) + 269
		*rest = (*rest)[2:]
		return v, nil
	case 15:
		return 0, ErrOptionDelta
	default:
		return int(nibble), nil
	}
}
