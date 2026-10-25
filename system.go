package goactor

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/tommasoprandin/goactor/codec"
	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/mailbox"
	"github.com/tommasoprandin/goactor/transport"
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
	cfg   *config.Config
	view  NodeView
	out   outbound

	mu      sync.RWMutex
	actors  map[ActorID]actorHandle
	pending map[ActorID]pendingReply
	stopped bool
	wg      sync.WaitGroup

	nacksDropped atomic.Uint64
}

// Need this because go can't store generic types with different
// instatiations together
type inbox interface {
	enqueue(msg any) error
}

type actorHandle interface {
	inbox
	stop()
}

type pendingReply interface {
	inbox
	fail(err error)
}

// NodeView is the local opinion of which nodes are running. Remote sends consult it before the
// transport.
type NodeView interface {
	IsAlive(NodeID) bool
	Epoch(NodeID) (uint32, bool)
}

// staticView reports every configured node alive and knows no epochs.
type staticView struct{ cfg *config.Config }

func (v staticView) IsAlive(id NodeID) bool {
	_, err := v.cfg.Node(id)
	return err == nil
}

func (staticView) Epoch(NodeID) (uint32, bool) { return 0, false }

type outbound interface {
	Start(h transport.Handler, services ...func(grpc.ServiceRegistrar)) error
	Send(to NodeID, e *codec.Envelope) error
	Close() error
}

// A Nack's payload is one byte indexing nackErrs.
var nackErrs = []error{ErrActorDead, ErrUnknownType, ErrTypeMismatch}

var current atomic.Pointer[System]

// Start runs node id of cfg under a fresh epoch taken from cfg.DataDir, and connects it to the
// other configured nodes. A process runs at most one System at a time.
func Start(cfg *config.Config, id NodeID) (*System, error) {
	return start(cfg, id, staticView{cfg}, transport.New(cfg, id))
}

func start(cfg *config.Config, id NodeID, view NodeView, out outbound) (*System, error) {
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
		cfg:     cfg,
		view:    view,
		out:     out,
		actors:  make(map[ActorID]actorHandle),
		pending: make(map[ActorID]pendingReply),
	}
	if err := out.Start(s.receive); err != nil {
		return nil, err
	}
	if !current.CompareAndSwap(nil, s) {
		out.Close()
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
	if err := s.out.Close(); err != nil {
		slog.Warn("goactor: closing transport", "node", s.node, "err", err)
	}
	current.CompareAndSwap(s, nil)
}

func send[T any](s *System, to, from ActorID, msg T) error {
	if to.IsZero() {
		return ErrActorDead
	}
	name, payload, err := codec.Marshal(msg)
	if err != nil {
		return err
	}
	return s.route(&codec.Envelope{To: to, From: from, TypeName: name, Payload: payload})
}

func (s *System) route(e *codec.Envelope) error {
	to := e.To.Node
	if to == s.node {
		return s.deliver(e)
	}
	if _, err := s.cfg.Node(to); err != nil {
		return err
	}
	if !s.view.IsAlive(to) {
		return ErrNodeDown
	}
	return s.out.Send(to, e)
}

// deliver is the single delivery routine of local sends and inbound traffic alike.
func (s *System) deliver(e *codec.Envelope) error {
	if e.Kind == codec.Nack {
		s.nacked(e)
		return nil
	}
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

func (s *System) nacked(e *codec.Envelope) {
	s.mu.RLock()
	p, ok := s.pending[e.To]
	s.mu.RUnlock()
	if !ok || len(e.Payload) != 1 || int(e.Payload[0]) >= len(nackErrs) {
		s.nacksDropped.Add(1)
		return
	}
	p.fail(nackErrs[e.Payload[0]])
}

// receive handles an envelope from the transport, answering failures with a Nack.
func (s *System) receive(e *codec.Envelope) {
	err := s.deliver(e)
	if err == nil || e.From.IsZero() {
		return
	}
	code := slices.IndexFunc(nackErrs, func(n error) bool { return errors.Is(err, n) })
	if code < 0 {
		slog.Warn("goactor: dropping envelope", "to", e.To, "err", err)
		return
	}
	s.route(&codec.Envelope{Kind: codec.Nack, To: e.From, Payload: []byte{byte(code)}})
}

func (s *System) lookup(id ActorID) (inbox, error) {
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

func (s *System) addPending(h pendingReply) ActorID {
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

// Spawn creates an actor running b, constructed from args. It does not wait for the actor's Start,
// so a failing Start shows up as ErrActorDead on a later Send.
func Spawn[A, T any](ctx Context, b Behavior[A, T], args A, opts ...SpawnOpt) (Ref[T], error) {
	s := ctx.system()
	var o spawnOpts
	for _, opt := range opts {
		opt(&o)
	}
	if o.node != 0 && o.node != s.node {
		return Ref[T]{}, errNotImplemented
	}
	e, ok := lookupBehavior(b.name)
	if !ok {
		return Ref[T]{}, fmt.Errorf("%w: %q", ErrUnknownBehavior, b.name)
	}
	if t := reflect.TypeFor[T](); e.msgType != t {
		return Ref[T]{}, fmt.Errorf("%w: behavior %q receives %v, not %v", ErrTypeMismatch, b.name, e.msgType, t)
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
