package coap

import (
	"errors"
	"strings"
)

// OptionID 为 CoAP 选项编号（RFC 7252 §3.1 及 RFC 7959）。
type OptionID uint16

const (
	OptionIfMatch       OptionID = 1
	OptionURIHost       OptionID = 3
	OptionETag          OptionID = 4
	OptionIfNoneMatch   OptionID = 5
	OptionObserve       OptionID = 6 // RFC 7641
	OptionURIPort       OptionID = 7
	OptionLocationPath  OptionID = 8
	OptionURIPath       OptionID = 11
	OptionContentFormat OptionID = 12
	OptionMaxAge        OptionID = 14
	OptionURIQuery      OptionID = 15
	OptionAccept        OptionID = 17
	OptionLocationQuery OptionID = 20
	OptionBlock2        OptionID = 23 // RFC 7959
	OptionBlock1        OptionID = 27 // RFC 7959
	OptionSize2         OptionID = 28
	OptionProxyURI      OptionID = 35
	OptionProxyScheme   OptionID = 39
	OptionSize1         OptionID = 60
)

// ErrUintTooLarge 表示整数选项超过 4 字节。
var ErrUintTooLarge = errors.New("coap: uint option exceeds 4 bytes")

// Option 为单个 CoAP 选项。
type Option struct {
	ID    OptionID
	Value []byte
}

// Options 为选项集合（编码时按编号排序）。
type Options []Option

// Get 返回首个匹配选项的值。
func (o Options) Get(id OptionID) ([]byte, bool) {
	for _, opt := range o {
		if opt.ID == id {
			return opt.Value, true
		}
	}
	return nil, false
}

// GetAll 返回所有匹配选项的值。
func (o Options) GetAll(id OptionID) [][]byte {
	var out [][]byte
	for _, opt := range o {
		if opt.ID == id {
			out = append(out, opt.Value)
		}
	}
	return out
}

// GetUint 以无符号整数形式读取选项值。
func (o Options) GetUint(id OptionID) (uint32, bool) {
	v, ok := o.Get(id)
	if !ok {
		return 0, false
	}
	n, err := decodeUint(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

// GetString 以字符串形式读取选项值。
func (o Options) GetString(id OptionID) (string, bool) {
	v, ok := o.Get(id)
	if !ok {
		return "", false
	}
	return string(v), true
}

// Set 设置一个选项：替换同编号的首个选项并删除其余同编号项。
func (o *Options) Set(id OptionID, value []byte) {
	out := (*o)[:0]
	done := false
	for _, opt := range *o {
		if opt.ID == id {
			if done {
				continue
			}
			done = true
			out = append(out, Option{ID: id, Value: value})
			continue
		}
		out = append(out, opt)
	}
	if !done {
		out = append(out, Option{ID: id, Value: value})
	}
	*o = out
}

// Add 追加一个选项（允许同编号多个，如 URI-Path）。
func (o *Options) Add(id OptionID, value []byte) {
	*o = append(*o, Option{ID: id, Value: value})
}

// SetUint 以无符号整数形式设置选项。
func (o *Options) SetUint(id OptionID, v uint32) { o.Set(id, encodeUint(v)) }

// SetString 以字符串形式设置选项。
func (o *Options) SetString(id OptionID, s string) { o.Set(id, []byte(s)) }

// Remove 删除所有同编号选项。
func (o *Options) Remove(id OptionID) {
	out := (*o)[:0]
	for _, opt := range *o {
		if opt.ID != id {
			out = append(out, opt)
		}
	}
	*o = out
}

// Path 返回 URI-Path 选项拼接出的路径（如 "/a/b"），无路径时返回 "/"。
func (o Options) Path() string {
	segs := o.GetAll(OptionURIPath)
	if len(segs) == 0 {
		return "/"
	}
	parts := make([]string, len(segs))
	for i, s := range segs {
		parts[i] = string(s)
	}
	return "/" + strings.Join(parts, "/")
}

// SetPath 按 "/" 拆分路径并写入 URI-Path 选项（替换原有路径）。
func (o *Options) SetPath(path string) {
	o.Remove(OptionURIPath)
	for _, seg := range strings.Split(path, "/") {
		if seg == "" {
			continue
		}
		o.Add(OptionURIPath, []byte(seg))
	}
}

// encodeUint 将整数编码为最短大端字节串（0 编码为空串）。
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

// decodeUint 解码选项中的无符号整数；超过 4 字节报错。
func decodeUint(b []byte) (uint32, error) {
	if len(b) > 4 {
		return 0, ErrUintTooLarge
	}
	var v uint32
	for _, c := range b {
		v = v<<8 | uint32(c)
	}
	return v, nil
}
