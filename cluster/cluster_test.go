package cluster

import (
	"bytes"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"go.uber.org/goleak"

	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/internal/ident"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var (
	defaults = config.GossipConfig{
		ProbeInterval:  time.Second,
		ProbeTimeout:   500 * time.Millisecond,
		IndirectChecks: 3,
		SuspicionMult:  4,
	}
	fast = config.GossipConfig{
		ProbeInterval:  200 * time.Millisecond,
		ProbeTimeout:   100 * time.Millisecond,
		IndirectChecks: 3,
		SuspicionMult:  2,
	}
	patient = config.GossipConfig{
		ProbeInterval:  100 * time.Millisecond,
		ProbeTimeout:   50 * time.Millisecond,
		IndirectChecks: 3,
		SuspicionMult:  20,
	}
)

func suspicion(g config.GossipConfig) time.Duration {
	return time.Duration(g.SuspicionMult) * g.ProbeInterval
}

func newConfig(t *testing.T, n int, g config.GossipConfig) *config.Config {
	t.Helper()
	cfg := &config.Config{Gossip: g}
	for id := 1; id <= n; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		cfg.Cluster = append(cfg.Cluster, config.NodeConfig{ID: ident.NodeID(id), Gossip: l.Addr().String()})
	}
	return cfg
}

func start(t *testing.T, cfg *config.Config, id ident.NodeID, epoch uint32, tune func(*memberlist.Config)) *Cluster {
	t.Helper()
	c := New(cfg, id)
	if err := c.join(epoch, tune); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Leave() })
	return c
}

// startAll runs every configured node under epoch 1 and waits until they all see each other.
func startAll(t *testing.T, cfg *config.Config, tune func(*memberlist.Config)) []*Cluster {
	t.Helper()
	var cs []*Cluster
	for _, n := range cfg.Cluster {
		cs = append(cs, start(t, cfg, n.ID, 1, tune))
	}
	waitFor(t, 5*time.Second, "convergence", func() bool {
		for _, c := range cs {
			for _, n := range cfg.Cluster {
				if !c.IsAlive(n.ID) {
					return false
				}
			}
		}
		return true
	})
	return cs
}

// kill stops c without leaving, as a crash would.
func kill(c *Cluster) {
	c.once.Do(func() {
		c.stop()
		c.wg.Wait()
		c.ml.Shutdown()
	})
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("no %s after %v", what, d)
		}
		time.Sleep(time.Millisecond)
	}
}

func noTune(*memberlist.Config) {}

func TestMeta(t *testing.T) {
	m := Meta{NodeID: math.MaxUint16, Epoch: math.MaxUint32}
	b := m.encode()
	if len(b) > memberlist.MetaMaxSize {
		t.Fatalf("%d bytes, limit %d", len(b), memberlist.MetaMaxSize)
	}
	if got, ok := decodeMeta(b); !ok || got != m {
		t.Fatalf("decoded %+v, %v", got, ok)
	}
	if _, ok := decodeMeta(b[1:]); ok {
		t.Fatal("decoded a short meta")
	}
}

// detect kills one of three nodes and returns how long the other two take to notice.
func detect(t *testing.T, g config.GossipConfig) time.Duration {
	t.Helper()
	cs := startAll(t, newConfig(t, 3, g), noTune)
	window := time.Duration(g.SuspicionMult+2*len(cs)) * g.ProbeInterval
	begin := time.Now()
	kill(cs[2])
	waitFor(t, window, "detection", func() bool { return !cs[0].IsAlive(3) && !cs[1].IsAlive(3) })
	return time.Since(begin)
}

func TestDetection(t *testing.T) {
	short, long := detect(t, fast), detect(t, defaults)
	if short >= long {
		t.Fatalf("short timing took %v, defaults %v", short, long)
	}
}

func TestRestart(t *testing.T) {
	for _, tc := range []struct {
		name     string
		detected bool
	}{{"after detection", true}, {"before detection", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newConfig(t, 3, patient)
			cs := startAll(t, cfg, noTune)
			kill(cs[2])
			if tc.detected {
				waitFor(t, 2*suspicion(patient), "detection", func() bool { return !cs[0].IsAlive(3) })
			}
			start(t, cfg, 3, 2, noTune)
			waitFor(t, 5*time.Second, "new epoch", func() bool {
				for _, c := range cs[:2] {
					if e, _ := c.Epoch(3); !c.IsAlive(3) || e != 2 {
						return false
					}
				}
				return true
			})
		})
	}
}

// cutTransport drops traffic to and from the address in cut. The interface is embedded because
// NetTransport's WriteToAddress would bypass WriteTo. Streams are cut too, or memberlist's TCP
// fallback ping would keep the node from being suspected.
type cutTransport struct {
	memberlist.Transport
	self string
	cut  *atomic.Pointer[string]
}

func (c *cutTransport) blocked(addr string) bool {
	a := c.cut.Load()
	return a != nil && (*a == c.self || *a == addr)
}

func (c *cutTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	if c.blocked(addr) {
		return time.Now(), nil
	}
	return c.Transport.WriteTo(b, addr)
}

func (c *cutTransport) DialTimeout(addr string, d time.Duration) (net.Conn, error) {
	if c.blocked(addr) {
		return nil, errors.New("cut")
	}
	return c.Transport.DialTimeout(addr, d)
}

// refuteLog records whether a refutation was logged.
type refuteLog struct{ seen atomic.Bool }

func (l *refuteLog) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte("Refuting a suspect message")) {
		l.seen.Store(true)
	}
	return len(b), nil
}

// startCuttable is startAll over transports sharing the returned cut. Node 3 logs to w: peers
// suspected by a cut node refute as well, so only its own log shows that it was suspected.
func startCuttable(t *testing.T, cfg *config.Config, w io.Writer) ([]*Cluster, *atomic.Pointer[string]) {
	t.Helper()
	cut := new(atomic.Pointer[string])
	return startAll(t, cfg, func(mc *memberlist.Config) {
		if mc.Name == nodeName(3) {
			mc.Logger = log.New(w, "", 0)
		}
		nt, err := memberlist.NewNetTransport(&memberlist.NetTransportConfig{
			BindAddrs: []string{mc.BindAddr},
			BindPort:  mc.BindPort,
			Logger:    mc.Logger,
		})
		if err != nil {
			t.Fatal(err)
		}
		self := net.JoinHostPort(mc.BindAddr, strconv.Itoa(mc.BindPort))
		mc.Transport = &cutTransport{nt, self, cut}
	}), cut
}

func TestSuspicionRefuted(t *testing.T) {
	cfg := newConfig(t, 3, patient)
	logs := new(refuteLog)
	cs, cut := startCuttable(t, cfg, logs)

	cut.Store(&cfg.Cluster[2].Gossip)
	defer time.AfterFunc(500*time.Millisecond, func() { cut.Store(nil) }).Stop()
	for deadline := time.Now().Add(suspicion(patient) + time.Second); time.Now().Before(deadline); {
		if !cs[0].IsAlive(3) || !cs[1].IsAlive(3) {
			t.Fatal("suspected node reported dead")
		}
		time.Sleep(time.Millisecond)
	}
	if !logs.seen.Load() {
		t.Fatal("no refutation: the node was never suspected")
	}
	for _, c := range cs[:2] {
		if e, ok := c.Epoch(3); !ok || e != 1 {
			t.Fatalf("epoch %d, %v", e, ok)
		}
	}
}

func TestRejoinHealsPartition(t *testing.T) {
	cfg := newConfig(t, 3, fast)
	cs, cut := startCuttable(t, cfg, io.Discard)

	cut.Store(&cfg.Cluster[2].Gossip)
	waitFor(t, 5*time.Second, "detection", func() bool { return !cs[0].IsAlive(3) && !cs[2].IsAlive(1) })
	cut.Store(nil)
	waitFor(t, 5*time.Second, "rejoin", func() bool { return cs[0].IsAlive(3) && cs[2].IsAlive(1) })
	if e, _ := cs[0].Epoch(3); e != 1 {
		t.Fatalf("epoch %d", e)
	}
}

func TestUnknownNameIgnored(t *testing.T) {
	cfg := newConfig(t, 2, fast)
	c := start(t, cfg, 1, 1, noTune)
	start(t, cfg, 2, 9, func(mc *memberlist.Config) { mc.Name = "rogue" })

	waitFor(t, 5*time.Second, "rogue member", func() bool { return c.ml.NumMembers() == 2 })
	if c.IsAlive(2) {
		t.Fatal("rogue reported alive")
	}
	if e, ok := c.Epoch(2); ok {
		t.Fatalf("rogue reported epoch %d", e)
	}
}

func TestLeave(t *testing.T) {
	cs := startAll(t, newConfig(t, 3, patient), noTune)
	begin := time.Now()
	if err := cs[2].Leave(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, suspicion(patient), "departure", func() bool { return !cs[0].IsAlive(3) && !cs[1].IsAlive(3) })
	if d := time.Since(begin); d >= suspicion(patient) {
		t.Fatalf("departure took %v", d)
	}
}
