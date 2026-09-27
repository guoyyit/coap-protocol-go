package coap

import (
	"encoding/json"
	"sync"
	"time"
)

// DedupKey identifies a confirmable message from a peer.
type DedupKey struct {
	Addr      string
	MessageID uint16
}

// Dedup records recently seen confirmable messages so duplicates are
// processed once. It can snapshot/restore its state for crash recovery.
type Dedup struct {
	mu      sync.Mutex
	window  time.Duration
	entries map[DedupKey]time.Time
	now     func() time.Time
}

// NewDedup creates a deduplicator keeping entries for window.
func NewDedup(window time.Duration) *Dedup {
	return &Dedup{
		window:  window,
		entries: make(map[DedupKey]time.Time),
		now:     time.Now,
	}
}

// Seen reports whether key was already recorded; if not, records it.
// Returns true for duplicates (already seen).
func (d *Dedup) Seen(key DedupKey) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	d.purgeLocked(now)
	if _, dup := d.entries[key]; dup {
		return true
	}
	d.entries[key] = now
	return false
}

// purgeLocked drops entries older than the window.
func (d *Dedup) purgeLocked(now time.Time) {
	for k, ts := range d.entries {
		if now.Sub(ts) > d.window {
			delete(d.entries, k)
		}
	}
}

// dedupSnapshot is the persisted form.
type dedupSnapshot struct {
	Window  string               `json:"window"`
	SavedAt time.Time            `json:"savedAt"`
	Entries []dedupSnapshotEntry `json:"entries"`
}

type dedupSnapshotEntry struct {
	Addr      string    `json:"addr"`
	MessageID uint16    `json:"mid"`
	SeenAt    time.Time `json:"seenAt"`
}

// Snapshot serializes current state for persistence.
func (d *Dedup) Snapshot() []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	d.purgeLocked(now)
	snap := dedupSnapshot{
		Window:  d.window.String(),
		SavedAt: now,
		Entries: make([]dedupSnapshotEntry, 0, len(d.entries)),
	}
	for k, ts := range d.entries {
		snap.Entries = append(snap.Entries, dedupSnapshotEntry{
			Addr: k.Addr, MessageID: k.MessageID, SeenAt: ts,
		})
	}
	data, _ := json.Marshal(snap)
	return data
}

// Restore loads state previously saved with Snapshot. Entries whose
// recorded age (relative to the snapshot's own clock) exceeds the window
// are discarded; restored entries keep their original timestamps so a
// restart does not resurrect ancient duplicates as new.
func (d *Dedup) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var snap dedupSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	for _, e := range snap.Entries {
		// Age at snapshot time plus time elapsed since snapshot.
		age := snap.SavedAt.Sub(e.SeenAt) + now.Sub(snap.SavedAt)
		if age > d.window {
			continue // safely stale: treat as never seen
		}
		d.entries[DedupKey{Addr: e.Addr, MessageID: e.MessageID}] = e.SeenAt
	}
	return nil
}
