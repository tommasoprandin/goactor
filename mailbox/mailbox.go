// Package mailbox implements an unbounded multi-producer, single-consumer queue.
// Implementation uses a double buffer splitting the "public" inbound from the "private"
// goroutine-owned buffer. Once the internal is empty, the slices are swapped.
package mailbox

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrMailboxClosed = errors.New("mailbox: closed")

type Mailbox[T any] struct {
	mu     sync.Mutex
	outer  []T
	inner  []T
	ipos   int
	notify chan struct{}
	closed bool
	depth  atomic.Int64
}

func New[T any](initialCap int) *Mailbox[T] {
	return &Mailbox[T]{
		outer:  make([]T, 0, initialCap),
		inner:  make([]T, 0, initialCap),
		notify: make(chan struct{}, 1),
	}
}

func (m *Mailbox[T]) Enqueue(v T) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrMailboxClosed
	}
	m.outer = append(m.outer, v)
	m.depth.Add(1)
	m.mu.Unlock()
	m.wake()
	return nil
}

// Next must be called from a single goroutine. It returns false once stop is closed, even with
// messages still queued, or once the mailbox is closed and drained. A nil stop never fires.
func (m *Mailbox[T]) Next(stop <-chan struct{}) (T, bool) {
	var zero T
	for {
		select {
		case <-stop:
			return zero, false
		default:
		}
		if m.ipos < len(m.inner) {
			v := m.inner[m.ipos]
			m.ipos++
			m.depth.Add(-1)
			return v, true
		}
		// if inner is empty swap inner and outer
		clear(m.inner)
		m.mu.Lock()
		m.inner, m.outer = m.outer, m.inner[:0]
		closed := m.closed
		m.mu.Unlock()
		m.ipos = 0
		// there is something on the new inner, read it
		if len(m.inner) > 0 {
			continue
		}
		if closed {
			return zero, false
		}
		// if mailbox empty block on either a notification or stop
		select {
		case <-m.notify:
		case <-stop:
			return zero, false
		}
	}
}

func (m *Mailbox[T]) Depth() int64 {
	return m.depth.Load()
}

// Close rejects further Enqueue calls. Messages already queued remain retrievable through Next.
func (m *Mailbox[T]) Close() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.wake()
}

func (m *Mailbox[T]) wake() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}
