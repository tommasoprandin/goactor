package goactor

import (
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		t0 := time.Now()
		var got []int
		var fired time.Duration
		r := spawnProbe(t, s, hooks{
			start: func(c *ActorContext[int]) { c.After(time.Second, 1) },
			receive: func(i int) {
				got = append(got, i)
				if i == 1 {
					fired = time.Since(t0)
				}
			},
		})
		r.Send(0)
		time.Sleep(2 * time.Second)
		r.Send(2)
		synctest.Wait()
		s.Shutdown()
		if !slices.Equal(got, []int{0, 1, 2}) || fired < time.Second {
			t.Fatalf("got %v, timer fired at %v", got, fired)
		}
	})
}

func TestAfterCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		cancels := make(chan [2]CancelFunc, 1)
		var got []int
		spawnProbe(t, s, hooks{
			start: func(c *ActorContext[int]) {
				cancels <- [2]CancelFunc{c.After(time.Second, 1), c.After(time.Second, 2)}
			},
			receive: func(i int) { got = append(got, i) },
		})
		cs := <-cancels
		cs[0]()
		cs[0]()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		cs[1]()
		s.Shutdown()
		if !slices.Equal(got, []int{2}) {
			t.Fatalf("got %v", got)
		}
	})
}

func TestAfterDeadActor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSystem(t)
		var got []int
		r := spawnProbe(t, s, hooks{
			start: func(c *ActorContext[int]) { c.After(time.Second, 1) },
			receive: func(i int) {
				got = append(got, i)
				panic("boom")
			},
		})
		r.Send(0)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if err := r.Send(3); !errors.Is(err, ErrActorDead) {
			t.Fatalf("send: %v", err)
		}
		s.Shutdown()
		if !slices.Equal(got, []int{0}) {
			t.Fatalf("got %v", got)
		}
	})
}
