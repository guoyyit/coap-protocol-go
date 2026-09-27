package coap

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// Notification is one observe push from the server.
type Notification struct {
	Message *Message
	// Seq is the observe sequence number; gaps reveal loss/reorder.
	Seq uint32
}

// Observation is a client-side subscription handle.
type Observation struct {
	// C delivers notifications; closed when the observation ends.
	C chan Notification

	client *Client
	addr   string
	token  []byte
	once   sync.Once
}

// Cancel unsubscribes; no further notifications are delivered, and the
// server is told (best effort) to stop pushing.
func (o *Observation) Cancel() {
	o.once.Do(func() {
		o.client.observers.remove(string(o.token))
		close(o.C)
		// Best-effort explicit deregister: GET with Observe=1.
		dereg := &Message{Token: o.token, Code: CodeGET}
		dereg.SetUint(OptionObserve, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = o.client.Do(ctx, dereg, o.addr)
	})
}

// observeRegistry tracks client-side observations by token.
type observeRegistry struct {
	mu    sync.Mutex
	byTok map[string]*Observation
}

func newObserveRegistry() *observeRegistry {
	return &observeRegistry{byTok: make(map[string]*Observation)}
}

// dispatch delivers an inbound notification to the matching observation.
// Returns true if the message was consumed as a notification.
func (r *observeRegistry) dispatch(msg *Message, addr string) bool {
	seq, ok := msg.GetUint(OptionObserve)
	if !ok {
		return false
	}
	r.mu.Lock()
	obs := r.byTok[string(msg.Token)]
	r.mu.Unlock()
	if obs == nil {
		return false
	}
	n := Notification{Message: msg, Seq: seq}
	select {
	case obs.C <- n:
	default: // slow consumer: drop rather than block the receive loop
	}
	return true
}

func (r *observeRegistry) add(token string, o *Observation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byTok[token] = o
}

func (r *observeRegistry) remove(token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byTok, token)
}

// Observe registers a GET with Observe=0 and returns the subscription.
// Notifications arrive on Observation.C with monotonically increasing
// sequence numbers; gaps indicate loss, duplicates are suppressed.
func (c *Client) Observe(ctx context.Context, msg *Message, addr string) (*Observation, error) {
	msg.SetUint(OptionObserve, 0)
	if len(msg.Token) == 0 {
		c.mu.Lock()
		msg.Token = c.newTokenLocked()
		c.mu.Unlock()
	}
	obs := &Observation{
		C:      make(chan Notification, 16),
		client: c,
		addr:   addr,
		token:  append([]byte(nil), msg.Token...),
	}
	c.observers.add(string(msg.Token), obs)
	resp, err := c.Do(ctx, msg, addr)
	if err != nil {
		c.observers.remove(string(msg.Token))
		return nil, err
	}
	// The registration response itself is the first notification.
	if seq, ok := resp.GetUint(OptionObserve); ok {
		obs.C <- Notification{Message: resp, Seq: seq}
	}
	return obs, nil
}

// serverObservation is one server-side subscriber.
type serverObservation struct {
	Addr  string `json:"addr"`
	Token []byte `json:"token"`
	Path  string `json:"path"`
	Seq   uint32 `json:"seq"`
	// LastSeen orders liveness; entries not refreshed within the
	// server ObserverTTL are considered dead and are not notified.
	LastSeen time.Time `json:"lastSeen"`
}

// observeStore persists subscriptions across restarts.
type observeStore struct {
	mu   sync.Mutex
	subs map[string]*serverObservation // key: addr|token
}

func newObserveStore() *observeStore {
	return &observeStore{subs: make(map[string]*serverObservation)}
}

func observeKey(addr string, token []byte) string {
	return addr + "|" + string(token)
}

func (s *observeStore) add(addr, path string, token []byte) *serverObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := observeKey(addr, token)
	if o, ok := s.subs[key]; ok {
		o.LastSeen = time.Now()
		return o
	}
	o := &serverObservation{
		Addr:     addr,
		Token:    append([]byte(nil), token...),
		Path:     path,
		LastSeen: time.Now(),
	}
	s.subs[key] = o
	return o
}

func (s *observeStore) remove(addr string, token []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, observeKey(addr, token))
}

// forPath returns a snapshot of live subscribers of path.
func (s *observeStore) forPath(path string, ttl time.Duration) []*serverObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []*serverObservation
	for key, o := range s.subs {
		if now.Sub(o.LastSeen) > ttl {
			delete(s.subs, key) // dead subscriber: prune, never notify again
			continue
		}
		if o.Path == path {
			out = append(out, o)
		}
	}
	return out
}

// observeSnapshot is the persisted form.
type observeSnapshot struct {
	SavedAt time.Time            `json:"savedAt"`
	Subs    []*serverObservation `json:"subs"`
}

// Snapshot serializes subscriptions for crash recovery.
func (s *observeStore) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := observeSnapshot{SavedAt: time.Now(), Subs: make([]*serverObservation, 0, len(s.subs))}
	for _, o := range s.subs {
		cp := *o
		snap.Subs = append(snap.Subs, &cp)
	}
	data, _ := json.Marshal(snap)
	return data
}

// Restore reloads subscriptions saved with Snapshot. Sequence numbers are
// preserved so post-restart notifications remain ordered with pre-crash
// ones; subscribers that were already stale stay dead.
func (s *observeStore) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var snap observeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range snap.Subs {
		s.subs[observeKey(o.Addr, o.Token)] = o
	}
	return nil
}
