// Package cluster tells which configured nodes are running, using SWIM through
// hashicorp/memberlist. The view is eventually consistent.
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/internal/ident"
)

const (
	metaSize     = 6
	leaveTimeout = 5 * time.Second
)

// Meta is what a node publishes alongside its liveness, fixed at Join.
type Meta struct {
	NodeID ident.NodeID
	Epoch  uint32
}

func (m Meta) encode() []byte {
	b := binary.BigEndian.AppendUint16(make([]byte, 0, metaSize), uint16(m.NodeID))
	return binary.BigEndian.AppendUint32(b, m.Epoch)
}

func decodeMeta(b []byte) (Meta, bool) {
	if len(b) != metaSize {
		return Meta{}, false
	}
	return Meta{ident.NodeID(binary.BigEndian.Uint16(b)), binary.BigEndian.Uint32(b[2:])}, true
}

// Cluster is one node's membership in the configured cluster.
type Cluster struct {
	cfg  *config.Config
	self ident.NodeID
	ml   *memberlist.Memberlist
	view *view
	stop context.CancelFunc
	wg   sync.WaitGroup
	once sync.Once
}

// New returns the membership of node self, with no node alive. It holds no resources until Join.
func New(cfg *config.Config, self ident.NodeID) *Cluster {
	return &Cluster{cfg: cfg, self: self, view: newView(cfg)}
}

// Join starts gossiping under epoch and returns without waiting for any peer.
func (c *Cluster) Join(epoch uint32) error {
	return c.join(epoch, func(*memberlist.Config) {})
}

func (c *Cluster) join(epoch uint32, tune func(*memberlist.Config)) error {
	n, err := c.cfg.Node(c.self)
	if err != nil {
		return err
	}
	addr, err := net.ResolveTCPAddr("tcp", n.Gossip)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}

	mc := memberlist.DefaultLANConfig()
	mc.Name = nodeName(c.self)
	mc.BindAddr = addr.IP.String()
	mc.BindPort = addr.Port
	mc.ProbeInterval = c.cfg.Gossip.ProbeInterval
	mc.ProbeTimeout = c.cfg.Gossip.ProbeTimeout
	mc.IndirectChecks = c.cfg.Gossip.IndirectChecks
	mc.SuspicionMult = c.cfg.Gossip.SuspicionMult
	mc.Delegate = delegate(Meta{c.self, epoch}.encode())
	mc.Events = c.view
	mc.Logger = slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug)
	tune(mc)

	if c.ml, err = memberlist.Create(mc); err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	c.stop = stop
	c.wg.Go(func() { c.rejoin(ctx) })
	return nil
}

// rejoin contacts every configured node not alive. memberlist never does: the first node up finds
// no peer, and neither side probes a node it has declared dead, so a healed partition stays split.
func (c *Cluster) rejoin(ctx context.Context) {
	t := time.NewTicker(c.cfg.Gossip.ProbeInterval)
	defer t.Stop()
	for {
		for _, n := range c.cfg.Cluster {
			if ctx.Err() != nil {
				return
			}
			if !c.IsAlive(n.ID) {
				c.ml.Join([]string{n.Gossip})
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// IsAlive reports whether id is running, as far as this node knows. A suspected node is alive.
func (c *Cluster) IsAlive(id ident.NodeID) bool {
	c.view.mu.RLock()
	defer c.view.mu.RUnlock()
	return c.view.nodes[id].alive
}

// Epoch returns the last epoch known for id, even if id is dead. It is false for a node never seen.
func (c *Cluster) Epoch(id ident.NodeID) (uint32, bool) {
	c.view.mu.RLock()
	defer c.view.mu.RUnlock()
	s, ok := c.view.nodes[id]
	return s.epoch, ok
}

// Leave announces the departure to the peers, so they need no suspicion timeout, and stops
// gossiping.
func (c *Cluster) Leave() error {
	var err error
	c.once.Do(func() {
		c.stop()
		c.wg.Wait()
		err = errors.Join(c.ml.Leave(leaveTimeout), c.ml.Shutdown())
	})
	return err
}

func nodeName(id ident.NodeID) string { return "node-" + strconv.Itoa(int(id)) }

type state struct {
	alive bool
	epoch uint32
}

// view folds memberlist events into a table, because Members hands out nodes that memberlist keeps
// writing to. Callbacks run under memberlist's node lock and must not call back into it.
type view struct {
	names map[string]ident.NodeID
	mu    sync.RWMutex
	nodes map[ident.NodeID]state
}

func newView(cfg *config.Config) *view {
	names := make(map[string]ident.NodeID, cfg.N())
	for _, n := range cfg.Cluster {
		names[nodeName(n.ID)] = n.ID
	}
	return &view{names: names, nodes: make(map[ident.NodeID]state, cfg.N())}
}

func (v *view) NotifyJoin(n *memberlist.Node) { v.update(n) }

func (v *view) NotifyUpdate(n *memberlist.Node) { v.update(n) }

func (v *view) NotifyLeave(n *memberlist.Node) {
	v.mu.Lock()
	defer v.mu.Unlock()
	id := v.names[n.Name]
	if s, ok := v.nodes[id]; ok {
		s.alive = false
		v.nodes[id] = s
	}
}

func (v *view) update(n *memberlist.Node) {
	id, ok := v.names[n.Name]
	if !ok {
		slog.Warn("cluster: ignoring node absent from the configuration", "name", n.Name)
		return
	}
	m, ok := decodeMeta(n.Meta)
	if !ok || m.NodeID != id {
		slog.Warn("cluster: ignoring node with invalid metadata", "name", n.Name, "meta", n.Meta)
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.nodes[id] = state{alive: true, epoch: m.Epoch}
}

// delegate publishes the encoded Meta. The cluster carries no other payload.
type delegate []byte

func (d delegate) NodeMeta(int) []byte { return d }

func (delegate) NotifyMsg([]byte) {}

func (delegate) GetBroadcasts(int, int) [][]byte { return nil }

func (delegate) LocalState(bool) []byte { return nil }

func (delegate) MergeRemoteState([]byte, bool) {}
