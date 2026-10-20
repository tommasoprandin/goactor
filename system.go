package goactor

import (
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tommasoprandin/goactor/codec"
	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/mailbox"
)

const (
	mailboxCap      = 16
	shutdownTimeout = 5 * time.Second
)

// System is the runtime of one node.
type System struct {
	node  NodeID
	epoch uint32
	seq   SeqAllocator

	mu      sync.RWMutex
	actors  map[ActorID]actorHandle
	pending map[ActorID]inbox
	stopped bool
	wg      sync.WaitGroup
}

// Need this because go can't store generic types with different
// instatiations together
type inbox interface {
	enqueue(msg any) error
}

type actorHandle interface {
	inbox
	stop()
	depth() int64
}

var current atomic.Pointer[System]

// Start runs node id of cfg under a fresh epoch taken from cfg.DataDir. A process runs at most one
// System at a time.
func Start(cfg *config.Config, id NodeID) (*System, error) {
	if current.Load() != nil {
		return nil, errRunning
	}
	if _, err := cfg.Node(id); err != nil {
		return nil, err
	}
	epoch, err := LoadEpoch(cfg.DataDir, id)
	if err != nil {
		return nil, err
	}
	s := &System{
		node:    id,
		epoch:   epoch,
		actors:  make(map[ActorID]actorHandle),
		pending: make(map[ActorID]inbox),
	}
	if !current.CompareAndSwap(nil, s) {
		return nil, errRunning
	}
	return s, nil
}

// Root returns a Context for use outside any actor.
func (s *System) Root() Context { return rootContext{s} }

// Shutdown stops every actor and waits for them to exit, up to a bounded timeout. Later Spawn calls
// fail with ErrNodeDown.
func (s *System) Shutdown() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	hs := slices.Collect(maps.Values(s.actors))
	s.mu.Unlock()

	for _, h := range hs {
		h.stop()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		slog.Warn("goactor: shutdown timed out", "node", s.node)
	}
	current.CompareAndSwap(s, nil)
}

func (s *System) deliver(e *codec.Envelope) error {
	h, err := s.lookup(e.To)
	if err != nil {
		return err
	}
	msg, err := codec.Unmarshal(e.TypeName, e.Payload)
	if err != nil {
		return err
	}
	return h.enqueue(msg)
}

func (s *System) lookup(id ActorID) (inbox, error) {
	if id.Node != s.node {
		return nil, errNotImplemented
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if h, ok := s.actors[id]; ok {
		return h, nil
	}
	if h, ok := s.pending[id]; ok {
		return h, nil
	}
	return nil, ErrActorDead
}

func (s *System) addPending(h inbox) ActorID {
	id := ActorID{Node: s.node, Epoch: s.epoch, Seq: s.seq.Next()}
	s.mu.Lock()
	s.pending[id] = h
	s.mu.Unlock()
	return id
}

func (s *System) remove(id ActorID) {
	s.mu.Lock()
	delete(s.actors, id)
	delete(s.pending, id)
	s.mu.Unlock()
}

type SpawnOpt func(*spawnOpts)

type spawnOpts struct {
	node NodeID
}

// WithNode places the actor on node id.
func WithNode(id NodeID) SpawnOpt {
	return func(o *spawnOpts) { o.node = id }
}

// Spawn creates an actor running behavior, constructed from args. It does not wait for the actor's
// Start, so a failing Start shows up as ErrActorDead on a later Send.
func Spawn[T any](ctx Context, behavior string, args any, opts ...SpawnOpt) (Ref[T], error) {
	s := ctx.system()
	var o spawnOpts
	for _, opt := range opts {
		opt(&o)
	}
	if o.node != 0 && o.node != s.node {
		return Ref[T]{}, errNotImplemented
	}
	e, ok := lookupBehavior(behavior)
	if !ok {
		return Ref[T]{}, fmt.Errorf("%w: %q", ErrUnknownBehavior, behavior)
	}
	if t := reflect.TypeFor[T](); e.msgType != t {
		return Ref[T]{}, fmt.Errorf("%w: behavior %q receives %v, not %v", ErrTypeMismatch, behavior, e.msgType, t)
	}
	id, err := e.spawn(s, args)
	if err != nil {
		return Ref[T]{}, err
	}
	return Ref[T]{id: id}, nil
}

// Registered before the goroutine starts: Start may send to itself, and a panicking Start must not
// race its own registration.
func spawnLocal[T any](s *System, a Actor[T]) (ActorID, error) {
	c := &ActorContext[T]{sys: s, actor: a, mb: mailbox.New[T](mailboxCap), quit: make(chan struct{})}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ActorID{}, ErrNodeDown
	}
	c.self = ActorID{Node: s.node, Epoch: s.epoch, Seq: s.seq.Next()}
	s.actors[c.self] = c
	s.wg.Add(1)
	s.mu.Unlock()

	go c.run()
	return c.self, nil
}

// Impl actorHandle methods for any generic ActorContext[T] + other helper methods
func (c *ActorContext[T]) enqueue(msg any) error {
	m, ok := msg.(T)
	if !ok {
		return fmt.Errorf("%w: actor %v receives %v, got %T", ErrTypeMismatch, c.self, reflect.TypeFor[T](), msg)
	}
	if c.mb.Enqueue(m) != nil {
		return ErrActorDead
	}
	return nil
}

func (c *ActorContext[T]) stop() { c.once.Do(func() { close(c.quit) }) }

func (c *ActorContext[T]) depth() int64 { return c.mb.Depth() }

func (c *ActorContext[T]) run() {
	defer c.sys.wg.Done()
	if c.start() != nil {
		c.exit()
		return
	}
	var err error
	for err == nil {
		m, ok := c.mb.Next(c.quit)
		if !ok {
			break
		}
		err = c.receive(m)
	}
	c.exit()
	c.finish(err)
}

func (c *ActorContext[T]) exit() {
	c.sys.remove(c.self)
	c.mb.Close()
	c.timers.close()
}

func (c *ActorContext[T]) start() (err error) {
	defer c.catch("Start", &err)
	c.actor.Start(c)
	return nil
}

func (c *ActorContext[T]) receive(m T) (err error) {
	defer c.catch("Receive", &err)
	c.actor.Receive(c, m)
	return nil
}

func (c *ActorContext[T]) finish(cause error) {
	defer c.catch("Stop", nil)
	c.actor.Stop(c, cause)
}

func (c *ActorContext[T]) catch(hook string, err *error) {
	r := recover()
	if r == nil {
		return
	}
	slog.Error("goactor: actor panicked", "actor", c.self, "in", hook, "panic", r, "stack", string(debug.Stack()))
	if err != nil {
		*err = panicError(r)
	}
}

func panicError(r any) error {
	if err, ok := r.(error); ok {
		return fmt.Errorf("goactor: actor panicked: %w", err)
	}
	return fmt.Errorf("goactor: actor panicked: %v", r)
}
