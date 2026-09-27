package coap

import (
	"encoding/binary"
	"sort"
	"strings"
)

// Option is a single CoAP option. Options of the same number repeat.
type Option struct {
	Number uint16
	Value  []byte
}

// Message is one CoAP datagram payload (RFC 7252 section 3).
type Message struct {
	Type      MessageType
	Code      uint8
	MessageID uint16
	Token     []byte
	Options   []Option
	Payload   []byte
}

// Path returns the URI-Path options joined as a slash-separated path.
func (m *Message) Path() string {
	var parts []string
	for _, o := range m.Options {
		if o.Number == OptionURIPath {
			parts = append(parts, string(o.Value))
		}
	}
	return "/" + strings.Join(parts, "/")
}

// SetPath replaces URI-Path options from a slash-separated path.
func (m *Message) SetPath(path string) {
	m.Remove(OptionURIPath)
	for _, seg := range strings.Split(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue
		}
		m.Options = append(m.Options, Option{Number: OptionURIPath, Value: []byte(seg)})
	}
}

// GetUint returns the first option with the given number decoded as uint.
func (m *Message) GetUint(number uint16) (uint32, bool) {
	for _, o := range m.Options {
		if o.Number == number {
			return decodeUint(o.Value), true
		}
	}
	return 0, false
}

// SetUint sets (replacing) an option with a uint value, minimally encoded.
func (m *Message) SetUint(number uint16, v uint32) {
	m.Remove(number)
	m.Options = append(m.Options, Option{Number: number, Value: encodeUint(v)})
}

// GetBytes returns the first option value with the given number.
func (m *Message) GetBytes(number uint16) ([]byte, bool) {
	for _, o := range m.Options {
		if o.Number == number {
			return o.Value, true
		}
	}
	return nil, false
}

// Remove deletes all options with the given number.
func (m *Message) Remove(number uint16) {
	out := m.Options[:0]
	for _, o := range m.Options {
		if o.Number != number {
			out = append(out, o)
		}
	}
	m.Options = out
}

func encodeUint(v uint32) []byte {
	switch {
	case v == 0:
		return nil
	case v < 1<<8:
		return []byte{byte(v)}
	case v < 1<<16:
		return []byte{byte(v >> 8), byte(v)}
	case v < 1<<24:
		return []byte{byte(v >> 16), byte(v >> 8), byte(v)}
	default:
		return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
}

func decodeUint(b []byte) uint32 {
	var v uint32
	for _, c := range b {
		v = v<<8 | uint32(c)
	}
	return v
}

const payloadMarker = 0xFF

// Encode serializes the message to wire format.
func Encode(m *Message) ([]byte, error) {
	if len(m.Token) > MaxTokenLen {
		return nil, ErrInvalidTokenLen
	}
	if m.Type > RST {
		return nil, ErrInvalidVersion // unreachable for valid enum; guards cast misuse
	}
	head := byte(1)<<6 | byte(m.Type)<<4 | byte(len(m.Token))
	buf := make([]byte, 0, 4+len(m.Token)+len(m.Payload)+16)
	buf = append(buf, head, m.Code, byte(m.MessageID>>8), byte(m.MessageID))
	buf = append(buf, m.Token...)

	// Options must be emitted in ascending number order (delta encoding).
	opts := make([]Option, len(m.Options))
	copy(opts, m.Options)
	sort.SliceStable(opts, func(i, j int) bool { return opts[i].Number < opts[j].Number })

	var prev uint16
	for _, o := range opts {
		delta := uint32(o.Number) - uint32(prev)
		d, dext := encodeOptPart(delta)
		l, lext := encodeOptPart(uint32(len(o.Value)))
		buf = append(buf, d<<4|l)
		buf = append(buf, dext...)
		buf = append(buf, lext...)
		buf = append(buf, o.Value...)
		prev = o.Number
	}
	if len(m.Payload) > 0 {
		buf = append(buf, payloadMarker)
		buf = append(buf, m.Payload...)
	}
	return buf, nil
}

// encodeOptPart encodes a delta/length nibble plus extension bytes.
func encodeOptPart(v uint32) (nibble byte, ext []byte) {
	switch {
	case v < 13:
		return byte(v), nil
	case v < 269:
		return 13, []byte{byte(v - 13)}
	default:
		return 14, []byte{byte((v - 269) >> 8), byte(v - 269)}
	}
}

// Decode parses a datagram. Malformed input yields a typed error, never a panic.
func Decode(data []byte) (*Message, error) {
	if len(data) < 4 {
		return nil, ErrMessageTooShort
	}
	ver := data[0] >> 6
	if ver != 1 {
		return nil, ErrInvalidVersion
	}
	typ := MessageType(data[0] >> 4 & 0x3)
	tkl := int(data[0] & 0x0F)
	if tkl > MaxTokenLen {
		return nil, ErrInvalidTokenLen
	}
	if len(data) < 4+tkl {
		return nil, ErrMessageTruncated
	}
	m := &Message{
		Type:      typ,
		Code:      data[1],
		MessageID: binary.BigEndian.Uint16(data[2:4]),
	}
	if tkl > 0 {
		m.Token = make([]byte, tkl)
		copy(m.Token, data[4:4+tkl])
	}
	rest := data[4+tkl:]

	var prev uint32
	for len(rest) > 0 {
		b := rest[0]
		if b == payloadMarker {
			if len(rest) == 1 {
				return nil, ErrPayloadMarker
			}
			m.Payload = make([]byte, len(rest)-1)
			copy(m.Payload, rest[1:])
			rest = nil
			break
		}
		delta, n, err := decodeOptPart(b>>4, rest[1:])
		if err != nil {
			return nil, err
		}
		rest = rest[1+n:]
		length, n, err := decodeOptPart(b&0x0F, rest)
		if err != nil {
			return nil, err
		}
		rest = rest[n:]
		if uint32(len(rest)) < length {
			return nil, ErrOptionTruncated
		}
		prev += delta
		if prev > 0xFFFF {
			return nil, ErrInvalidOptionDelta
		}
		val := make([]byte, length)
		copy(val, rest[:length])
		m.Options = append(m.Options, Option{Number: uint16(prev), Value: val})
		rest = rest[length:]
	}

	// Empty messages (Code 0.00) must not carry token/options/payload.
	if m.Code == CodeEmpty && (tkl > 0 || len(m.Options) > 0 || len(m.Payload) > 0) {
		return nil, ErrInvalidTokenLen
	}
	return m, nil
}

// decodeOptPart decodes one 4-bit delta/length with its extension bytes.
// Returns the value and the number of extension bytes consumed.
func decodeOptPart(nibble byte, data []byte) (uint32, int, error) {
	switch nibble {
	case 13:
		if len(data) < 1 {
			return 0, 0, ErrOptionTruncated
		}
		return uint32(data[0]) + 13, 1, nil
	case 14:
		if len(data) < 2 {
			return 0, 0, ErrOptionTruncated
		}
		return (uint32(data[0])<<8 | uint32(data[1])) + 269, 2, nil
	case 15:
		return 0, 0, ErrInvalidOptionDelta
	default:
		return uint32(nibble), 0, nil
	}
}
