package goactor

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"go.uber.org/goleak"

	"github.com/tommasoprandin/goactor/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type hooks struct {
	start   func()
	receive func(int)
	stop    func(error)
}

type probe struct{ h hooks }

var probesBuilt atomic.Int64

func newProbe(h hooks) Actor[int] {
	probesBuilt.Add(1)
	return &probe{h}
}

func (p *probe) Start(*ActorContext[int]) {
	if p.h.start != nil {
		p.h.start()
	}
}

func (p *probe) Receive(_ *ActorContext[int], m int) {
	if p.h.receive != nil {
		p.h.receive(m)
	}
}

func (p *probe) Stop(_ *ActorContext[int], err error) {
	if p.h.stop != nil {
		p.h.stop(err)
	}
}

type ball struct {
	N    int
	From Ref[ball]
}

type pingPong struct{ done chan struct{} }

func (p *pingPong) Start(*ActorContext[ball]) {}

func (p *pingPong) Receive(ctx *ActorContext[ball], b ball) {
	if b.N == 0 {
		close(p.done)
		return
	}
	b.From.Send(ball{N: b.N - 1, From: ctx.SelfRef()})
}

func (p *pingPong) Stop(*ActorContext[ball], error) {}

func init() {
	Register("test.probe", newProbe)
	Register("test.pingpong", func(done chan struct{}) Actor[ball] { return &pingPong{done} })
}

func newSystem(t *testing.T) *System {
	t.Helper()
	cfg := &config.Config{
		Cluster: []config.NodeConfig{{ID: 1, Gossip: "127.0.0.1:7946", GRPC: "127.0.0.1:9000"}},
		DataDir: t.TempDir(),
	}
	s, err := Start(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Shutdown)
	return s
}

func spawnProbe(t *testing.T, s *System, h hooks) Ref[int] {
	t.Helper()
	r, err := Spawn[int](s.Root(), "test.probe", h)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		var got []string
		r := spawnProbe(t, s, hooks{
			start:   func() { got = append(got, "start") },
			receive: func(i int) { got = append(got, fmt.Sprint(i)) },
			stop:    func(err error) { got = append(got, fmt.Sprint("stop ", err)) },
		})
		want := []string{"start"}
		for i := range 100 {
			if err := r.Send(i); err != nil {
				t.Fatal(err)
			}
			want = append(want, fmt.Sprint(i))
		}
		want = append(want, "stop <nil>")
		synctest.Wait()
		s.Shutdown()
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestDepth(t *testing.T) {
	s := newSystem(t)
	gate := make(chan struct{})
	defer close(gate)
	r := spawnProbe(t, s, hooks{start: func() { <-gate }})
	for i := range 3 {
		r.Send(i)
	}
	h, err := s.lookup(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if d := h.depth(); d != 3 {
		t.Fatalf("depth %d, want 3", d)
	}
}

func TestTypeMismatch(t *testing.T) {
	s := newSystem(t)
	got := make(chan int, 1)
	r := spawnProbe(t, s, hooks{receive: func(i int) { got <- i }})

	if err := (Ref[string]{id: r.ID(), sys: s}).Send("x"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("wrong message type: %v", err)
	}
	if err := r.Send(7); err != nil {
		t.Fatal(err)
	}
	if v := <-got; v != 7 {
		t.Fatalf("got %d", v)
	}

	built := probesBuilt.Load()
	if _, err := Spawn[int](s.Root(), "test.probe", "not hooks"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("wrong args: %v", err)
	}
	if _, err := Spawn[string](s.Root(), "test.probe", hooks{}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("wrong T: %v", err)
	}
	if n := probesBuilt.Load(); n != built {
		t.Fatalf("constructor ran %d times", n-built)
	}
}

func TestPanicInReceiveAndStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		var stops []error
		r := spawnProbe(t, s, hooks{
			receive: func(int) { panic("boom") },
			stop: func(err error) {
				stops = append(stops, err)
				panic("again")
			},
		})
		var sibling []int
		sib := spawnProbe(t, s, hooks{receive: func(i int) { sibling = append(sibling, i) }})

		r.Send(1)
		synctest.Wait()
		if err := r.Send(2); !errors.Is(err, ErrActorDead) {
			t.Fatalf("send to dead actor: %v", err)
		}
		if err := sib.Send(3); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		s.Shutdown()
		if len(stops) != 1 || stops[0] == nil || !strings.Contains(stops[0].Error(), "boom") {
			t.Fatalf("stops %v", stops)
		}
		if !slices.Equal(sibling, []int{3}) {
			t.Fatalf("sibling got %v", sibling)
		}
	})
}

func TestPanicInStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		stopped := false
		r := spawnProbe(t, s, hooks{
			start: func() { panic("boom") },
			stop:  func(error) { stopped = true },
		})
		synctest.Wait()
		if err := r.Send(1); !errors.Is(err, ErrActorDead) {
			t.Fatalf("send: %v", err)
		}
		s.Shutdown()
		if stopped {
			t.Fatal("Stop called")
		}
	})
}

func TestRegisterDuplicatePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	Register("test.probe", newProbe)
}

func TestConcurrentSpawn(t *testing.T) {
	s := newSystem(t)
	ids := make([]ActorID, 1000)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			r, err := Spawn[int](s.Root(), "test.probe", hooks{})
			if err != nil {
				t.Error(err)
			}
			ids[i] = r.ID()
		})
	}
	wg.Wait()
	seen := make(map[ActorID]bool, len(ids))
	for _, id := range ids {
		if seen[id] || id.Node != 1 || id.Epoch != s.epoch || id.Seq == 0 {
			t.Fatalf("bad or duplicate id %v", id)
		}
		seen[id] = true
	}
}

func TestPingPong(t *testing.T) {
	s := newSystem(t)
	done := make(chan struct{})
	a, err := Spawn[ball](s.Root(), "test.pingpong", done)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Spawn[ball](s.Root(), "test.pingpong", done)
	if err != nil {
		t.Fatal(err)
	}
	a.Send(ball{N: 100_000, From: b})
	<-done
}

func TestShutdown(t *testing.T) {
	s := newSystem(t)
	var stops atomic.Int64
	for range 100 {
		spawnProbe(t, s, hooks{stop: func(err error) {
			if err == nil {
				stops.Add(1)
			}
		}})
	}
	s.Shutdown()
	if n := stops.Load(); n != 100 {
		t.Fatalf("%d graceful stops, want 100", n)
	}
	if _, err := Spawn[int](s.Root(), "test.probe", hooks{}); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("spawn after shutdown: %v", err)
	}
}
