package goactor

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

// Request sends msg to r and waits up to timeout for the reply. T must be a struct with a field
// ReplyTo of type Ref[R]; Request overwrites it with a single-use reply address, to which the
// responder sends the reply.
//
// Request blocks the calling actor's goroutine, which processes no other message meanwhile, so
// request cycles between actors deadlock. Use Send with After for non-blocking behaviour.
func Request[T, R any](ctx Context, r Ref[T], msg T, timeout time.Duration) (R, error) {
	var zero R
	field, err := replyField[T, R]()
	if err != nil {
		return zero, err
	}
	s := ctx.system()
	slot := make(replySlot[R], 1)
	id := s.addPending(slot)
	defer s.remove(id)
	// Need to write into msg the reply address
	reflect.ValueOf(&msg).Elem().Field(field).Set(reflect.ValueOf(Ref[R]{id: id, sys: s}))
	if err := r.Send(msg); err != nil {
		return zero, err
	}
	select {
	case v := <-slot:
		return v, nil
	case <-time.After(timeout):
		return zero, ErrTimeout
	}
}

type replySlot[R any] chan R

func (s replySlot[R]) enqueue(msg any) error {
	m, ok := msg.(R)
	if !ok {
		return fmt.Errorf("%w: reply is %v, got %T", ErrTypeMismatch, reflect.TypeFor[R](), msg)
	}
	select {
	case s <- m:
		return nil
	default:
		return ErrActorDead
	}
}

type replyFieldEntry struct {
	index int
	err   error
}

var replyFields = struct {
	sync.RWMutex
	m map[[2]reflect.Type]replyFieldEntry
}{m: make(map[[2]reflect.Type]replyFieldEntry)}

// replyField returns the index of T's ReplyTo field of type Ref[R], or an error if T has none.
// Both outcomes are cached per (T, R).
func replyField[T, R any]() (int, error) {
	key := [2]reflect.Type{reflect.TypeFor[T](), reflect.TypeFor[Ref[R]]()}
	replyFields.RLock()
	e, ok := replyFields.m[key]
	replyFields.RUnlock()
	if !ok {
		e = resolveReplyField(key[0], key[1])
		replyFields.Lock()
		replyFields.m[key] = e
		replyFields.Unlock()
	}
	return e.index, e.err
}

func resolveReplyField(msg, ref reflect.Type) replyFieldEntry {
	if msg.Kind() == reflect.Struct {
		if f, ok := msg.FieldByName("ReplyTo"); ok && len(f.Index) == 1 && f.Type == ref {
			return replyFieldEntry{index: f.Index[0]}
		}
	}
	return replyFieldEntry{err: fmt.Errorf("%w: %v has no field ReplyTo %v", ErrTypeMismatch, msg, ref)}
}
