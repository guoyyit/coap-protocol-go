package coap

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	m := &Message{
		Type:      CON,
		Code:      CodeGET,
		MessageID: 0xBEEF,
		Token:     []byte{1, 2, 3, 4},
		Payload:   []byte("hello"),
	}
	m.SetPath("/sensors/temp")
	m.SetUint(OptionObserve, 0)
	m.SetUint(OptionBlock2, 1027) // exercises 2-byte uint

	data, err := Encode(m)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != CON || got.Code != CodeGET || got.MessageID != 0xBEEF {
		t.Fatalf("header mismatch: %+v", got)
	}
	if !bytes.Equal(got.Token, m.Token) {
		t.Fatalf("token mismatch: %v", got.Token)
	}
	if got.Path() != "/sensors/temp" {
		t.Fatalf("path mismatch: %q", got.Path())
	}
	if v, ok := got.GetUint(OptionBlock2); !ok || v != 1027 {
		t.Fatalf("block2 mismatch: %v %v", v, ok)
	}
	if !bytes.Equal(got.Payload, []byte("hello")) {
		t.Fatalf("payload mismatch: %q", got.Payload)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"too short", []byte{0x40, 0x01, 0x00}, ErrMessageTooShort},
		{"bad version", []byte{0x80, 0x01, 0x00, 0x01}, ErrInvalidVersion},
		{"tkl 9 reserved", []byte{0x49, 0x01, 0x00, 0x01}, ErrInvalidTokenLen},
		{"token truncated", []byte{0x44, 0x01, 0x00, 0x01, 0xAA}, ErrMessageTruncated},
		{"delta 15 reserved", []byte{0x40, 0x01, 0x00, 0x01, 0xF0}, ErrInvalidOptionDelta},
		{"length 15 reserved", []byte{0x40, 0x01, 0x00, 0x01, 0x0F}, ErrInvalidOptionDelta},
		{"ext byte truncated", []byte{0x40, 0x01, 0x00, 0x01, 0xD0}, ErrOptionTruncated},
		{"value truncated", []byte{0x40, 0x01, 0x00, 0x01, 0x14, 0xAA}, ErrOptionTruncated},
		{"marker no payload", []byte{0x40, 0x01, 0x00, 0x01, 0xFF}, ErrPayloadMarker},
		{"empty msg with token", []byte{0x41, 0x00, 0x00, 0x01, 0xAA}, ErrInvalidTokenLen},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeDoesNotPanicOnGarbage(t *testing.T) {
	// Fuzz-ish: every truncation of a valid message must error, not panic.
	m := &Message{Type: CON, Code: CodeGET, MessageID: 7, Token: []byte{9}}
	m.SetPath("/a/b")
	m.Payload = []byte("xyz")
	data, _ := Encode(m)
	for i := 0; i < len(data); i++ {
		_, _ = Decode(data[:i]) // must not panic
	}
	// Random byte flips.
	for i := 0; i < len(data); i++ {
		for b := 0; b < 256; b += 37 {
			cp := append([]byte(nil), data...)
			cp[i] = byte(b)
			_, _ = Decode(cp) // must not panic
		}
	}
}

func TestOptionDeltaExtended(t *testing.T) {
	// Option number jump requiring 2-byte extended delta (>= 269).
	m := &Message{Type: NON, Code: CodeContent, MessageID: 1}
	m.Options = append(m.Options, Option{Number: 3, Value: []byte("h")})
	m.Options = append(m.Options, Option{Number: 300, Value: []byte("x")})
	data, err := Encode(m)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Options) != 2 || got.Options[1].Number != 300 {
		t.Fatalf("options mismatch: %+v", got.Options)
	}
}
