package goactor

import (
	"errors"
	"sync"

	"github.com/tommasoprandin/goactor/mailbox"
)

var (
	ErrActorDead       = errors.New("goactor: actor dead")
	ErrTypeMismatch    = errors.New("goactor: type mismatch")
	ErrUnknownBehavior = errors.New("goactor: unknown behavior")
	ErrNodeDown        = errors.New("goactor: node down")
	ErrTimeout         = errors.New("goactor: timeout")

	errNotImplemented = errors.New("goactor: not implemented")
)

// Actor is a behaviour receiving messages of type T. The runtime calls every method on the actor's
// own goroutine, so they never interleave.
type Actor[T any] interface {
	// Start runs once, before the first message. A panic kills the actor without calling Stop.
	Start(ctx *ActorContext[T])
	Receive(ctx *ActorContext[T], msg T)
	// Stop runs once for every actor whose Start returned. err wraps the recovered value if the
	// actor panicked, and is nil otherwise.
	Stop(ctx *ActorContext[T], err error)
}

// Context is the handle package-level functions such as Spawn act through. It is implemented by
// *ActorContext[T] and returned by System.Root.
type Context interface {
	Self() ActorID
	system() *System
}

// ActorContext is an actor's handle on the runtime, passed to its hooks. It is valid only on the
// actor's own goroutine.
type ActorContext[T any] struct {
	sys    *System
	self   ActorID
	actor  Actor[T]
	mb     *mailbox.Mailbox[T]
	quit   chan struct{}
	once   sync.Once
	timers timerSet
}

func (c *ActorContext[T]) Self() ActorID { return c.self }

func (c *ActorContext[T]) SelfRef() Ref[T] { return Ref[T]{id: c.self, sys: c.sys} }

func (c *ActorContext[T]) system() *System { return c.sys }

type rootContext struct{ sys *System }

func (rootContext) Self() ActorID { return ActorID{} }

func (c rootContext) system() *System { return c.sys }

// Ref addresses an actor receiving messages of type T.
type Ref[T any] struct {
	id  ActorID
	sys *System
}

func (r Ref[T]) ID() ActorID { return r.id }

// Send enqueues msg without blocking. A nil error means the message was accepted, not processed.
func (r Ref[T]) Send(msg T) error {
	if r.sys == nil {
		return ErrActorDead
	}
	h, err := r.sys.lookup(r.id)
	if err != nil {
		return err
	}
	return h.enqueue(msg)
}
