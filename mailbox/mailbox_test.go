package mailbox

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestOrder(t *testing.T) {
	const n = 10_000
	type item struct{ p, i int }
	for _, producers := range []int{1, 8} {
		t.Run(fmt.Sprintf("producers=%d", producers), func(t *testing.T) {
			m := New[item](16)
			var wg sync.WaitGroup
			for p := range producers {
				wg.Go(func() {
					for i := range n {
						m.Enqueue(item{p, i})
					}
				})
			}
			next := make([]int, producers)
			for range producers * n {
				v, ok := m.Next(nil)
				if !ok {
					t.Fatal("Next returned false")
				}
				if v.i != next[v.p] {
					t.Fatalf("producer %d: got %d, want %d", v.p, v.i, next[v.p])
				}
				next[v.p]++
			}
			wg.Wait()
		})
	}
}

func TestStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New[int](0)
		stop := make(chan struct{})
		done := make(chan bool, 1)
		go func() {
			_, ok := m.Next(stop)
			done <- ok
		}()
		synctest.Wait()
		close(stop)
		synctest.Wait()
		select {
		case ok := <-done:
			if ok {
				t.Fatal("Next returned true on an empty mailbox")
			}
		default:
			t.Fatal("Next still blocked after stop")
		}

		m.Enqueue(1)
		if _, ok := m.Next(stop); ok {
			t.Fatal("Next served a message after stop")
		}
	})
}

func TestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New[int](0)
		for i := range 3 {
			m.Enqueue(i)
		}
		m.Close()
		if err := m.Enqueue(3); !errors.Is(err, ErrMailboxClosed) {
			t.Fatalf("Enqueue after Close: %v", err)
		}
		for i := range 3 {
			if v, ok := m.Next(nil); !ok || v != i {
				t.Fatalf("got %d, %v, want %d, true", v, ok, i)
			}
		}
		if _, ok := m.Next(nil); ok {
			t.Fatal("Next returned true on a closed, drained mailbox")
		}

		m = New[int](0)
		done := make(chan bool, 1)
		go func() {
			_, ok := m.Next(nil)
			done <- ok
		}()
		synctest.Wait()
		m.Close()
		synctest.Wait()
		select {
		case ok := <-done:
			if ok {
				t.Fatal("Next returned true on a closed, empty mailbox")
			}
		default:
			t.Fatal("Next still blocked after Close")
		}
	})
}

func TestNoLostWakeup(t *testing.T) {
	m := New[int](0)
	stop := make(chan struct{})
	watchdog := time.AfterFunc(30*time.Second, func() { close(stop) })
	defer watchdog.Stop()
	release := make(chan int)
	defer close(release)
	go func() {
		for i := range release {
			m.Enqueue(i)
		}
	}()
	for i := range 100_000 {
		release <- i
		if v, ok := m.Next(stop); !ok || v != i {
			t.Fatalf("iteration %d: got %d, %v", i, v, ok)
		}
	}
}

func TestRecycledBuffersCleared(t *testing.T) {
	m := New[*int](0)
	for i := range 100 {
		m.Enqueue(&i)
	}
	m.Close()
	for range 100 {
		m.Next(nil)
	}
	if _, ok := m.Next(nil); ok {
		t.Fatal("Next returned true on a closed, drained mailbox")
	}
	for _, buf := range [][]*int{m.inner, m.outer} {
		if slices.ContainsFunc(buf[:cap(buf)], func(p *int) bool { return p != nil }) {
			t.Fatal("recycled buffer retains references")
		}
	}
}

func BenchmarkEnqueueDequeue(b *testing.B) {
	for _, producers := range []int{1, 8} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			b.ReportAllocs()
			m := New[int](64)
			var wg sync.WaitGroup
			for p := range producers {
				n := b.N / producers
				if p == 0 {
					n += b.N % producers
				}
				wg.Go(func() {
					for i := range n {
						m.Enqueue(i)
					}
				})
			}
			for range b.N {
				m.Next(nil)
			}
			wg.Wait()
		})
	}
}
