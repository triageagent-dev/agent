package agentui

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/push"
)

// publishEvery is how often the leader sends its view to the other replicas.
const publishEvery = 5 * time.Second

// wireState is what the leader sends: the Store plus what only its Pusher
// knows (the apps and the connection to the core).
type wireState struct {
	Snapshot Snapshot           `json:"snapshot"`
	Apps     []*push.VirtualApp `json:"apps"`
	Link     push.Link          `json:"link"`
	Host     bool               `json:"host"`
	Tenants  map[string]string  `json:"tenants"`
	From     string             `json:"from,omitempty"`
	Ledger   []ledger.Entry     `json:"ledger,omitempty"`
	Drops    []ledger.Drop      `json:"drops,omitempty"`
}

// Replica makes every replica show the leader's view. Only the leader's
// Pusher and collectors run, so only its Store fills; the leader sends it to
// the others over the replica gossip every few seconds and they serve what
// they receive. It implements Link, so the Server reads apps and connection
// state from the leader's copy on a follower.
type Replica struct {
	local   Link
	store   *Store
	publish func([]byte)
	self    string
	log     logr.Logger

	leader atomic.Bool

	mu     sync.RWMutex
	remote wireState
}

// NewReplica returns a Replica over local and store. publish sends a payload
// to the other replicas (mesh.PublishUI), nil when there is no gossip.
func NewReplica(local Link, store *Store, self string, publish func([]byte), log logr.Logger) *Replica {
	return &Replica{local: local, store: store, publish: publish, self: self, log: log}
}

// Start runs on the leader only (NeedLeaderElection): it sends the view until
// ctx ends.
func (r *Replica) Start(ctx context.Context) error {
	r.leader.Store(true)
	defer r.leader.Store(false)
	if r.publish == nil {
		<-ctx.Done()
		return nil
	}
	t := time.NewTicker(publishEvery)
	defer t.Stop()
	for {
		r.send()
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// NeedLeaderElection reports that Start publishes, so it waits for the lease.
func (r *Replica) NeedLeaderElection() bool { return true }

func (r *Replica) send() {
	b, err := json.Marshal(wireState{Snapshot: r.store.Export(), Apps: r.local.Apps(), Link: r.local.Link(),
		Host: r.local.Host(), Tenants: r.local.AppTenants(), From: r.self, Ledger: ledger.Default.Snapshot(), Drops: ledger.Default.Drops()})
	if err != nil {
		r.log.Error(err, "encode TUI state")
		return
	}
	r.publish(b)
}

// Apply takes the leader's view from a peer. A leader ignores it.
func (r *Replica) Apply(b []byte) {
	if r.leader.Load() {
		return
	}
	var w wireState
	if err := json.Unmarshal(b, &w); err != nil {
		r.log.Error(err, "decode TUI state")
		return
	}
	r.store.Import(w.Snapshot)
	r.mu.Lock()
	r.remote = w
	r.mu.Unlock()
}

// Role is "leader", "follower" or "single" when there is no gossip.
func (r *Replica) Role() string {
	switch {
	case r.publish == nil:
		return "single"
	case r.leader.Load():
		return "leader"
	}
	return "follower"
}

// Apps returns the leader's apps.
func (r *Replica) Apps() []*push.VirtualApp {
	if r.leader.Load() || r.publish == nil {
		return r.local.Apps()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Apps
}

// Link returns the leader's connection state.
func (r *Replica) Link() push.Link {
	if r.leader.Load() || r.publish == nil {
		return r.local.Link()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Link
}

// NoKubernetes reports whether the agent watches no Kubernetes objects. It is
// a start-up setting, the same on every replica, so the local answer holds.
func (r *Replica) NoKubernetes() bool {
	nk, ok := r.local.(interface{ NoKubernetes() bool })
	return ok && nk.NoKubernetes()
}

// Host reports host mode.
func (r *Replica) Host() bool {
	if r.leader.Load() || r.publish == nil {
		return r.local.Host()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Host
}

// AppTenants returns the leader's namespace-to-tenant map.
func (r *Replica) AppTenants() map[string]string {
	if r.leader.Load() || r.publish == nil {
		return r.local.AppTenants()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Tenants
}

// Ledger returns the leader's signal-path counters: the collectors run on the
// leader only, so a follower shows the copy it was sent.
func (r *Replica) Ledger() []ledger.Entry {
	if r.leader.Load() || r.publish == nil {
		return ledger.Default.Snapshot()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Ledger
}

// Drops returns the leader's held-back entries: the collectors run on the
// leader only, so a follower shows the copy it was sent.
func (r *Replica) Drops() []ledger.Drop {
	if r.leader.Load() || r.publish == nil {
		return ledger.Default.Drops()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Drops
}
