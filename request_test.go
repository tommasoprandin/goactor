package goactor

import (
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

type query struct {
	N       int
	ReplyTo Ref[int]
}

type echo struct{ delay time.Duration }

func (e *echo) Start(*ActorContext[query]) {}

func (e *echo) Receive(_ *ActorContext[query], q query) {
	time.Sleep(e.delay)
	q.ReplyTo.Send(2 * q.N)
}

func (e *echo) Stop(*ActorContext[query], error) {}

var echoBehavior = Register("test.echo", func(d time.Duration) Actor[query] { return &echo{d} })

func spawnEcho(t *testing.T, s *System, delay time.Duration) Ref[query] {
	t.Helper()
	r, err := Spawn(s.Root(), echoBehavior, delay, WithNode(s.node))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func pendingLen(s *System) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pending)
}

func TestRequest(t *testing.T) {
	s := newSystem(t)
	v, err := Request[query, int](s.Root(), spawnEcho(t, s, 0), query{N: 21}, time.Second)
	if err != nil || v != 42 {
		t.Fatalf("got %d, %v", v, err)
	}
}

func TestRequestTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		r := spawnEcho(t, s, 2*time.Second)
		t0 := time.Now()
		_, err := Request[query, int](s.Root(), r, query{N: 1}, time.Second)
		if d := time.Since(t0); !errors.Is(err, ErrTimeout) || d < time.Second || d > 1100*time.Millisecond {
			t.Fatalf("err %v after %v", err, d)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if n := pendingLen(s); n != 0 {
			t.Fatalf("%d pending", n)
		}
	})
}

func TestRequestErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		dead := Ref[query]{id: ActorID{Node: 1, Epoch: s.epoch, Seq: 1 << 40}}
		t0 := time.Now()
		if _, err := Request[query, int](s.Root(), dead, query{}, time.Hour); !errors.Is(err, ErrActorDead) || time.Since(t0) != 0 {
			t.Fatalf("dead actor: %v after %v", err, time.Since(t0))
		}
		if _, err := Request[int, int](s.Root(), spawnProbe(t, s, hooks{}), 1, time.Hour); !errors.Is(err, ErrTypeMismatch) {
			t.Fatalf("no ReplyTo: %v", err)
		}
		if n := pendingLen(s); n != 0 {
			t.Fatalf("%d pending", n)
		}
	})
}

func TestConcurrentRequests(t *testing.T) {
	s := newSystem(t)
	e := spawnEcho(t, s, 0)
	const n = 1000
	errs := make(chan error, n)
	for i := range n {
		spawnProbe(t, s, hooks{start: func(c *ActorContext[int]) {
			v, err := Request[query, int](c, e, query{N: i}, 10*time.Second)
			if err == nil && v != 2*i {
				err = fmt.Errorf("request %d: got %d", i, v)
			}
			errs <- err
		}})
	}
	for range n {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	if n := pendingLen(s); n != 0 {
		t.Fatalf("%d pending", n)
	}
}
