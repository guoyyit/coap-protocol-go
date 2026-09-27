package coap

import (
	"container/list"
	"sync"
	"time"
)

// 默认去重参数：窗口取 RFC 7252 的 EXCHANGE_LIFETIME（247s）。
const (
	defaultDedupWindow     = 247 * time.Second
	defaultDedupMaxEntries = 4096
)

// DedupConfig 控制去重窗口。
type DedupConfig struct {
	// Window 为条目保留时长（默认 247s，即 EXCHANGE_LIFETIME）。
	Window time.Duration
	// MaxEntries 为容量上限，超出时淘汰最旧条目。
	MaxEntries int
}

func (c DedupConfig) withDefaults() DedupConfig {
	if c.Window <= 0 {
		c.Window = defaultDedupWindow
	}
	if c.MaxEntries <= 0 {
		c.MaxEntries = defaultDedupMaxEntries
	}
	return c
}

// dedupKey 以（来源地址，消息编号）唯一标识一次可确认交换。
type dedupKey struct {
	from Addr
	mid  uint16
}

type dedupEntry struct {
	key  dedupKey
	resp *Message // 缓存的响应（深拷贝），可为 nil
	at   time.Time
}

// Dedup 记录已处理的可确认报文（按来源+消息编号），
// 用于幂等去重与缓存响应以便重发。并发安全。
type Dedup struct {
	mu      sync.Mutex
	cfg     DedupConfig
	entries map[dedupKey]*list.Element
	lru     *list.List // 前端为最新
}

// NewDedup 创建去重器。
func NewDedup(cfg DedupConfig) *Dedup {
	return &Dedup{
		cfg:     cfg.withDefaults(),
		entries: make(map[dedupKey]*list.Element),
		lru:     list.New(),
	}
}

// Check 报告 (from, mid) 是否已出现过；
// 若曾缓存响应则一并返回（深拷贝），调用方应直接重发缓存响应。
func (d *Dedup) Check(from Addr, mid uint16) (dup bool, cached *Message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneLocked(time.Now())
	el, ok := d.entries[dedupKey{from, mid}]
	if !ok {
		return false, nil
	}
	ent := el.Value.(*dedupEntry)
	d.lru.MoveToFront(el)
	return true, ent.resp.Clone()
}

// Store 记录 (from, mid) 已处理，并缓存其响应（可为 nil）。
func (d *Dedup) Store(from Addr, mid uint16, resp *Message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	d.pruneLocked(now)
	key := dedupKey{from, mid}
	if el, ok := d.entries[key]; ok {
		ent := el.Value.(*dedupEntry)
		ent.resp = resp.Clone()
		ent.at = now
		d.lru.MoveToFront(el)
		return
	}
	el := d.lru.PushFront(&dedupEntry{key: key, resp: resp.Clone(), at: now})
	d.entries[key] = el
	for d.lru.Len() > d.cfg.MaxEntries {
		back := d.lru.Back()
		if back == nil {
			break
		}
		delete(d.entries, back.Value.(*dedupEntry).key)
		d.lru.Remove(back)
	}
}

// pruneLocked 惰性清理过期条目。
func (d *Dedup) pruneLocked(now time.Time) {
	for {
		back := d.lru.Back()
		if back == nil {
			return
		}
		ent := back.Value.(*dedupEntry)
		if now.Sub(ent.at) <= d.cfg.Window {
			return
		}
		delete(d.entries, ent.key)
		d.lru.Remove(back)
	}
}

// snapshot 导出内部状态用于持久化（崩溃恢复）。
func (d *Dedup) snapshot() []dedupRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneLocked(time.Now())
	var out []dedupRecord
	for el := d.lru.Front(); el != nil; el = el.Next() {
		ent := el.Value.(*dedupEntry)
		rec := dedupRecord{From: string(ent.key.from), MID: ent.key.mid, At: ent.at.UnixNano()}
		if ent.resp != nil {
			if data, err := ent.resp.Encode(); err == nil {
				rec.Resp = data
			}
		}
		out = append(out, rec)
	}
	return out
}

// restore 从持久化状态重建；已过期或损坏的记录被丢弃。
func (d *Dedup) restore(recs []dedupRecord) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	for _, r := range recs {
		at := time.Unix(0, r.At)
		if now.Sub(at) > d.cfg.Window {
			continue // 重启耗时超过窗口的记录不再有效
		}
		var resp *Message
		if len(r.Resp) > 0 {
			m, err := Decode(r.Resp)
			if err != nil {
				continue // 损坏记录不恢复
			}
			resp = m
		}
		key := dedupKey{Addr(r.From), r.MID}
		if _, ok := d.entries[key]; ok {
			continue
		}
		el := d.lru.PushFront(&dedupEntry{key: key, resp: resp, at: at})
		d.entries[key] = el
	}
	for d.lru.Len() > d.cfg.MaxEntries {
		back := d.lru.Back()
		if back == nil {
			break
		}
		delete(d.entries, back.Value.(*dedupEntry).key)
		d.lru.Remove(back)
	}
}

// dedupRecord 为可序列化的去重条目。
type dedupRecord struct {
	From string `json:"from"`
	MID  uint16 `json:"mid"`
	Resp []byte `json:"resp,omitempty"` // 缓存响应的编码字节，可为空
	At   int64  `json:"at"`             // 记录时间（unix 纳秒）
}
