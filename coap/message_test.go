package coap

import (
	"bytes"
	"errors"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	m := &Message{
		Type:      CON,
		Code:      GET,
		MessageID: 0xABCD,
		Token:     []byte{1, 2, 3, 4},
	}
	m.Options.SetPath("/sensors/temp")
	m.Options.SetUint(OptionContentFormat, 40)
	m.Payload = []byte("hello")

	data, err := m.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Type != CON || got.Code != GET || got.MessageID != 0xABCD {
		t.Fatalf("header mismatch: %+v", got)
	}
	if !bytes.Equal(got.Token, m.Token) {
		t.Fatalf("token mismatch: %v", got.Token)
	}
	if got.PathString() != "/sensors/temp" {
		t.Fatalf("path mismatch: %q", got.PathString())
	}
	if cf, ok := got.Options.GetUint(OptionContentFormat); !ok || cf != 40 {
		t.Fatalf("content-format mismatch: %v %v", cf, ok)
	}
	if !bytes.Equal(got.Payload, []byte("hello")) {
		t.Fatalf("payload mismatch: %q", got.Payload)
	}
}

func TestMessageRoundTripExtendedDelta(t *testing.T) {
	// Size1(60) 触发扩展 delta（60-15=45 > 13），覆盖扩展编解码。
	m := &Message{Type: NON, Code: PUT, MessageID: 1}
	m.Options.SetUint(OptionSize1, 70000) // 3 字节值
	m.Options.SetString(OptionURIPath, "x")
	data, err := m.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if v, ok := got.Options.GetUint(OptionSize1); !ok || v != 70000 {
		t.Fatalf("size1 mismatch: %v %v", v, ok)
	}
	if got.PathString() != "/x" {
		t.Fatalf("path mismatch: %q", got.PathString())
	}
}

func TestDecodeMalformed(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"empty", nil, ErrMessageTooShort},
		{"short3", []byte{0x40, 0x01, 0x00}, ErrMessageTooShort},
		{"badversion", []byte{0x80, 0x01, 0x00, 0x01}, ErrInvalidVersion},
		{"tkl9", []byte{0x49, 0x01, 0x00, 0x01, 1, 2, 3, 4, 5, 6, 7, 8, 9}, ErrInvalidTokenLen},
		{"tkl15", []byte{0x4F, 0x01, 0x00, 0x01}, ErrInvalidTokenLen},
		{"tokentrunc", []byte{0x44, 0x01, 0x00, 0x01, 1, 2}, ErrMessageTooShort},
		{"emptymsg-withtoken", []byte{0x41, 0x00, 0x00, 0x01, 0xAA}, ErrEmptyHasBody},
		{"emptymsg-withpayload", []byte{0x40, 0x00, 0x00, 0x01, 0xFF, 0x01}, ErrEmptyHasBody},
		{"opt-delta15", []byte{0x40, 0x01, 0x00, 0x01, 0xF1, 0x00}, ErrOptionDelta},
		{"opt-len15", []byte{0x40, 0x01, 0x00, 0x01, 0x1F, 0x00}, ErrOptionDelta},
		{"opt-ext-trunc", []byte{0x40, 0x01, 0x00, 0x01, 0xD1}, ErrOptionsTruncated},
		{"opt-ext2-trunc", []byte{0x40, 0x01, 0x00, 0x01, 0xE1, 0x00}, ErrOptionsTruncated},
		{"opt-val-trunc", []byte{0x40, 0x01, 0x00, 0x01, 0x15, 0x01, 0x02}, ErrOptionsTruncated},
		{"payload-marker-only", []byte{0x40, 0x01, 0x00, 0x01, 0xFF}, ErrPayloadMarker},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestDecodeEmptyMessage(t *testing.T) {
	m, err := Decode([]byte{0x40, 0x00, 0x12, 0x34})
	if err != nil {
		t.Fatalf("Decode empty: %v", err)
	}
	if m.Code != CodeEmpty || m.MessageID != 0x1234 || len(m.Token) != 0 {
		t.Fatalf("bad empty message: %+v", m)
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	if _, err := (&Message{Type: CON, Code: GET, Token: make([]byte, 9)}).Encode(); !errors.Is(err, ErrInvalidTokenLen) {
		t.Fatalf("want ErrInvalidTokenLen, got %v", err)
	}
	if _, err := (&Message{Type: CON, Code: CodeEmpty, Token: []byte{1}}).Encode(); !errors.Is(err, ErrEmptyHasBody) {
		t.Fatalf("want ErrEmptyHasBody, got %v", err)
	}
}

func TestUintCodec(t *testing.T) {
	for _, v := range []uint32{0, 1, 255, 256, 65535, 65536, 1<<24 - 1, 1 << 24, 1<<32 - 1} {
		got, err := decodeUint(encodeUint(v))
		if err != nil || got != v {
			t.Fatalf("uint codec mismatch for %d: %d %v", v, got, err)
		}
	}
	if _, err := decodeUint([]byte{1, 2, 3, 4, 5}); !errors.Is(err, ErrUintTooLarge) {
		t.Fatalf("want ErrUintTooLarge, got %v", err)
	}
}
