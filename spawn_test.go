package goactor

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tommasoprandin/goactor/config"
)

type aliveView map[NodeID]bool

func (v aliveView) IsAlive(id NodeID) bool { return v[id] }

func (aliveView) Epoch(NodeID) (uint32, bool) { return 0, false }

func threeNodes() *config.Config {
	return &config.Config{Cluster: []config.NodeConfig{{ID: 1}, {ID: 2}, {ID: 3}}}
}

// Children of the harness never register it, so it is unknown to every node but the test process.
var parentOnly Behavior[int, hop]

func init() {
	if os.Getenv(envNode) == "" {
		parentOnly = Register("test.parentOnly", func(tag int) Actor[hop] { return &relay{tag} })
	}
}

func TestPlace(t *testing.T) {
	for _, c := range []struct {
		alive aliveView
		want  []NodeID
	}{
		{aliveView{1: true, 2: true, 3: true}, []NodeID{1, 2, 3}},
		{aliveView{1: true, 3: true}, []NodeID{1, 3}},
		{aliveView{2: true}, []NodeID{2}},
	} {
		s := &System{cfg: threeNodes(), view: fixedMembers{c.alive}}
		s.next.Store(2)
		for i := range 3 * len(c.want) {
			got, err := s.place()
			if want := c.want[i%len(c.want)]; err != nil || got != want {
				t.Fatalf("%v, call %d: got %d, %v, want %d", c.alive, i, got, err, want)
			}
		}
	}

	s := &System{cfg: threeNodes(), view: fixedMembers{aliveView{}}}
	if _, err := s.place(); !errors.Is(err, ErrNoLiveNodes) {
		t.Fatalf("nobody alive: %v", err)
	}
}

func TestOwnerGolden(t *testing.T) {
	s := &System{cfg: threeNodes()}
	for name, want := range map[string]NodeID{"coordinator": 3, "a": 2, "worker-7": 1, "mandelbrot": 3} {
		if got := s.owner(name); got != want {
			t.Fatalf("owner(%q) = %d, want %d", name, got, want)
		}
	}
}

func TestWithNodeDead(t *testing.T) {
	out := new(fakeOut)
	s := startSystem(t, aliveView{1: true}, out)
	if _, err := Spawn(s.Root(), relayBehavior, 0, WithNode(2)); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("spawn: %v", err)
	}
	if n := out.conns.Load(); n != 0 {
		t.Fatalf("%d network calls", n)
	}
}

func nameOwnedBy(s *System, id NodeID) string {
	for i := 0; ; i++ {
		if name := fmt.Sprintf("test.name%d", i); s.owner(name) == id {
			return name
		}
	}
}

func TestDistributed(t *testing.T) {
	c := startCluster(t, 3, "relay")
	root := c.sys.Root()

	spread := make(map[NodeID]int)
	for range 30 {
		r, err := Spawn(root, relayBehavior, 0)
		if err != nil {
			t.Fatal(err)
		}
		spread[r.ID().Node]++
		if _, err := Request[hop, hop](root, r, hop{}, harnessTimeout); err != nil {
			t.Fatal(err)
		}
	}
	if spread[1] != 10 || spread[2] != 10 || spread[3] != 10 {
		t.Fatalf("spread %v", spread)
	}

	if r, err := Spawn(root, relayBehavior, 0, WithNode(3)); err != nil || r.ID().Node != 3 {
		t.Fatalf("pinned: %v, %v", r.ID(), err)
	}
	_, err := Spawn(root, parentOnly, 0, WithNode(2))
	if !errors.Is(err, ErrUnknownBehavior) || !strings.Contains(err.Error(), "test.parentOnly") {
		t.Fatalf("unknown behavior: %v", err)
	}

	name := nameOwnedBy(c.sys, 2)
	ids := make([]ActorID, 100)
	var created atomic.Int64
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			r, ok, err := SpawnNamed(root, name, relayBehavior, 0)
			if err != nil {
				t.Error(err)
			}
			if ok {
				created.Add(1)
			}
			ids[i] = r.ID()
		})
	}
	wg.Wait()
	if n := created.Load(); n != 1 {
		t.Fatalf("%d calls created the actor", n)
	}
	for _, id := range ids {
		if id != ids[0] || id.Node != 2 {
			t.Fatalf("got %v and %v", ids[0], id)
		}
	}

	r, err := Lookup[hop](root, name)
	if err != nil || r.ID() != ids[0] {
		t.Fatalf("lookup: %v, %v", r.ID(), err)
	}
	if _, err := Request[hop, hop](root, r, hop{}, harnessTimeout); err != nil {
		t.Fatal(err)
	}
	if _, err := Lookup[int](root, name); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("wrong type: %v", err)
	}
	if _, err := Lookup[hop](root, "test.unbound"); !errors.Is(err, ErrNameNotFound) {
		t.Fatalf("unbound: %v", err)
	}

	r.Send(hop{N: -1})
	if !eventually(func() bool {
		_, err := Lookup[hop](root, name)
		return errors.Is(err, ErrNameNotFound)
	}) {
		t.Fatal("name still bound after the actor panicked")
	}
	again, ok, err := SpawnNamed(root, name, relayBehavior, 0)
	if err != nil || !ok || again.ID() == ids[0] {
		t.Fatalf("respawn: %v, %v, %v", again.ID(), ok, err)
	}
}
