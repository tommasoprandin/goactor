package goactor

import (
	"sync"
	"time"
)

// CancelFunc stops a timer set by After. It is idempotent and safe to call after the timer fired.
type CancelFunc func()

// After sends msg to the actor itself once d has elapsed. Timers are cancelled when the actor dies.
func (c *ActorContext[T]) After(d time.Duration, msg T) CancelFunc {
	ref := c.SelfRef()
	return c.timers.after(d, func() { ref.Send(msg) })
}

type timerSet struct {
	mu     sync.Mutex
	m      map[uint64]*time.Timer
	next   uint64
	closed bool
}

func (s *timerSet) after(d time.Duration, f func()) CancelFunc {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return func() {}
	}
	if s.m == nil {
		s.m = make(map[uint64]*time.Timer)
	}
	s.next++
	id := s.next
	s.m[id] = time.AfterFunc(d, func() {
		if s.take(id) != nil {
			f()
		}
	})
	return func() {
		if t := s.take(id); t != nil {
			t.Stop()
		}
	}
}

func (s *timerSet) take(id uint64) *time.Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.m[id]
	delete(s.m, id)
	return t
}

func (s *timerSet) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, t := range s.m {
		t.Stop()
	}
	s.m = nil
}
