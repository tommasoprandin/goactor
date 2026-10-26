package goactor

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"maps"
	"math/rand/v2"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tommasoprandin/goactor/cluster"
	"github.com/tommasoprandin/goactor/codec"
	"github.com/tommasoprandin/goactor/config"
	"github.com/tommasoprandin/goactor/internal/wire"
	"github.com/tommasoprandin/goactor/mailbox"
	"github.com/tommasoprandin/goactor/transport"
)

const (
	mailboxCap      = 16
	shutdownTimeout = 5 * time.Second
	rpcTimeout      = 5 * time.Second
	namedRetry      = 250 * time.Millisecond
	namedTimeout    = 30 * time.Second
)

// System is the runtime of one node.
type System struct {
	node   NodeID
	epoch  uint32
	seq    SeqAllocator
	next   atomic.Uint64
	cfg    *config.Config
	view   membership
	out    outbound
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	actors  map[ActorID]actorHandle
	pending map[ActorID]pendingReply
	wg      sync.WaitGroup

	namesMu sync.Mutex
	names   map[string]nameEntry

	nacksDropped atomic.Uint64
}

type nameEntry struct {
	id      ActorID
	msgType string
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

// membership is a NodeView with a lifetime. It exists before the System joins, so inbound traffic
// can read it from the moment the transport serves.
type membership interface {
	NodeView
	Join(epoch uint32) error
	Leave() error
}

type outbound interface {
	Start(h transport.Handler, services ...func(grpc.ServiceRegistrar)) error
	Send(to NodeID, e *codec.Envelope) error
	Conn(to NodeID) (grpc.ClientConnInterface, error)
	Close() error
}

// A Nack's payload is one byte indexing nackErrs.
var nackErrs = []error{ErrActorDead, ErrUnknownType, ErrTypeMismatch}

// A Spawner response's ErrorCode indexes peerErrs.
var peerErrs = []error{
	wire.ErrorCode_OK:               nil,
	wire.ErrorCode_UNKNOWN_BEHAVIOR: ErrUnknownBehavior,
	wire.ErrorCode_TYPE_MISMATCH:    ErrTypeMismatch,
	wire.ErrorCode_NAME_NOT_FOUND:   ErrNameNotFound,
	wire.ErrorCode_NODE_DOWN:        ErrNodeDown,
	wire.ErrorCode_ACTOR_DEAD:       ErrActorDead,
}

var current atomic.Pointer[System]

// Start runs node id of cfg under a fresh epoch taken from cfg.DataDir, connects it to the other
// configured nodes and joins the cluster. A process runs at most one System at a time.
func Start(cfg *config.Config, id NodeID) (*System, error) {
	if current.Load() != nil {
		return nil, errRunning
	}
	return start(cfg, id, cluster.New(cfg, id), transport.New(cfg, id))
}

func start(cfg *config.Config, id NodeID, m membership, out outbound) (*System, error) {
	if _, err := cfg.Node(id); err != nil {
		return nil, err
	}
	epoch, err := LoadEpoch(cfg.DataDir, id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &System{
		node:    id,
		epoch:   epoch,
		cfg:     cfg,
		view:    m,
		out:     out,
		ctx:     ctx,
		cancel:  cancel,
		actors:  make(map[ActorID]actorHandle),
		pending: make(map[ActorID]pendingReply),
		names:   make(map[string]nameEntry),
	}
	s.next.Store(rand.Uint64())
	// Before serving: an actor spawned by a peer may Send from its Start.
	if !current.CompareAndSwap(nil, s) {
		cancel()
		return nil, errRunning
	}
	err = out.Start(s.receive, func(r grpc.ServiceRegistrar) { wire.RegisterSpawnerServer(r, spawner{s: s}) })
	if err == nil {
		if err = m.Join(epoch); err != nil {
			out.Close()
		}
	}
	if err != nil {
		cancel()
		current.Store(nil)
		return nil, err
	}
	return s, nil
}

// Root returns a Context for use outside any actor.
func (s *System) Root() Context { return rootContext{s} }

// Shutdown stops every actor and waits for them to exit, up to a bounded timeout, then leaves the
// cluster. Later Spawn calls fail with ErrNodeDown.
func (s *System) Shutdown() {
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	s.cancel()
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
	if err := s.view.Leave(); err != nil {
		slog.Warn("goactor: leaving cluster", "node", s.node, "err", err)
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

// unbind waits out a creation in progress, so an actor that dies at once leaves no stale entry.
func (s *System) unbind(name string) {
	s.namesMu.Lock()
	delete(s.names, name)
	s.namesMu.Unlock()
}

type SpawnOpt func(*spawnOpts)

type spawnOpts struct {
	node   NodeID
	pinned bool
}

// WithNode places the actor on node id, which must be alive, instead of the next node in turn.
func WithNode(id NodeID) SpawnOpt {
	return func(o *spawnOpts) { o.node, o.pinned = id, true }
}

// Spawn creates an actor running b, constructed from args, on the next alive node in round-robin
// order or on the node WithNode names. It does not wait for the actor's Start, so a failing Start
// shows up as ErrActorDead on a later Send.
//
// args are serialised only when the actor lands on another node, so an unencodable A must be pinned
// to the calling node. A remote spawn that fails with ErrTimeout may have left an orphan actor on
// the target.
func Spawn[A, T any](ctx Context, b Behavior[A, T], args A, opts ...SpawnOpt) (Ref[T], error) {
	s := ctx.system()
	var o spawnOpts
	for _, opt := range opts {
		opt(&o)
	}
	e, err := checkBehavior(b)
	if err != nil {
		return Ref[T]{}, err
	}
	to, err := s.target(o)
	if err != nil {
		return Ref[T]{}, err
	}
	if to == s.node {
		id, err := e.spawn(s, args, "")
		return Ref[T]{id: id}, err
	}
	req, err := spawnReq(b, "", args)
	if err != nil {
		return Ref[T]{}, err
	}
	id, _, err := s.callSpawn(to, req)
	return Ref[T]{id: id}, err
}

// SpawnNamed returns the actor bound to name on the name's owner, creating it from args if the name
// is unbound. The boolean reports whether this call created it; if a response is lost, possibly no
// caller sees true. Every caller gets the same actor, however many race. args are always serialised.
//
// While the owner is unavailable it retries for up to 30 s, then fails with ErrRegistryUnavailable.
// It fails with ErrNodeDown if the local System stops first.
func SpawnNamed[A, T any](ctx Context, name string, b Behavior[A, T], args A) (Ref[T], bool, error) {
	if name == "" {
		return Ref[T]{}, false, fmt.Errorf("%w: empty name", ErrNameNotFound)
	}
	if _, err := checkBehavior(b); err != nil {
		return Ref[T]{}, false, err
	}
	req, err := spawnReq(b, name, args)
	if err != nil {
		return Ref[T]{}, false, err
	}
	id, created, err := ctx.system().spawnNamed(req)
	return Ref[T]{id: id}, created, err
}

// Lookup returns the actor bound to name, failing with ErrNameNotFound if the name is unbound and
// with ErrTypeMismatch if the actor does not receive T. It makes one attempt: an unavailable owner
// is ErrRegistryUnavailable.
func Lookup[T any](ctx Context, name string) (Ref[T], error) {
	n, err := ctx.system().lookupName(name)
	if err != nil {
		return Ref[T]{}, err
	}
	if t := reflect.TypeFor[T](); n.msgType != codec.TypeName(t) {
		return Ref[T]{}, fmt.Errorf("%w: %q receives %s, not %v", ErrTypeMismatch, name, n.msgType, t)
	}
	return Ref[T]{id: n.id}, nil
}

func spawnReq[A, T any](b Behavior[A, T], name string, args A) (*wire.SpawnReq, error) {
	typeName, payload, err := codec.Marshal(args)
	if err != nil {
		return nil, err
	}
	return &wire.SpawnReq{Behavior: b.name, ArgsType: typeName, Args: payload, Name: name}, nil
}

func (s *System) target(o spawnOpts) (NodeID, error) {
	if s.ctx.Err() != nil {
		return 0, ErrNodeDown
	}
	if !o.pinned {
		return s.place()
	}
	_, err := s.cfg.Node(o.node)
	return o.node, err
}

func (s *System) place() (NodeID, error) {
	n := uint64(len(s.cfg.Cluster))
	for range n {
		id := s.cfg.Cluster[s.next.Add(1)%n].ID
		if s.view.IsAlive(id) {
			return id, nil
		}
	}
	return 0, ErrNoLiveNodes
}

func (s *System) owner(name string) NodeID {
	h := fnv.New32a()
	h.Write([]byte(name))
	return s.cfg.Cluster[h.Sum32()%uint32(len(s.cfg.Cluster))].ID
}

func (s *System) spawnNamed(req *wire.SpawnReq) (ActorID, bool, error) {
	to := s.owner(req.Name)
	giveUp := time.After(namedTimeout)
	for {
		id, created, err := s.callSpawn(to, req)
		err = ownerErr(err, req.Name, to)
		if !errors.Is(err, ErrRegistryUnavailable) {
			return id, created, err
		}
		select {
		case <-s.ctx.Done():
			return ActorID{}, false, ErrNodeDown
		case <-giveUp:
			return ActorID{}, false, err
		case <-time.After(namedRetry):
		}
	}
}

func (s *System) lookupName(name string) (nameEntry, error) {
	to := s.owner(name)
	resp, err := call(s, to, &wire.LookupReq{Name: name}, s.handleLookup, wire.SpawnerClient.Lookup)
	if err == nil {
		err = peerErr(resp.Error)
	}
	if err != nil {
		return nameEntry{}, ownerErr(err, name, to)
	}
	return nameEntry{resp.Id.Ident(), resp.MsgType}, nil
}

func ownerErr(err error, name string, to NodeID) error {
	if errors.Is(err, ErrNoRoute) || errors.Is(err, ErrTimeout) || errors.Is(err, ErrNodeDown) {
		return fmt.Errorf("%w: owner of %q is node %d: %v", ErrRegistryUnavailable, name, to, err)
	}
	return err
}

func (s *System) callSpawn(to NodeID, req *wire.SpawnReq) (ActorID, bool, error) {
	resp, err := call(s, to, req, s.handleSpawn, wire.SpawnerClient.Spawn)
	if err != nil {
		return ActorID{}, false, err
	}
	err = peerErr(resp.Error)
	if errors.Is(err, ErrUnknownBehavior) {
		err = fmt.Errorf("%w: %q on node %d: nodes run mismatched binaries", err, req.Behavior, to)
	}
	return resp.Id.Ident(), resp.Created, err
}

// call runs a Spawner handler of node to, directly if that is this node.
func call[Req, Resp any](
	s *System, to NodeID, req *Req,
	here func(*Req) *Resp,
	there func(wire.SpawnerClient, context.Context, *Req, ...grpc.CallOption) (*Resp, error),
) (*Resp, error) {
	if to == s.node {
		return here(req), nil
	}
	if !s.view.IsAlive(to) {
		return nil, ErrNodeDown
	}
	conn, err := s.out.Conn(to)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.ctx, rpcTimeout)
	defer cancel()
	resp, err := there(wire.NewSpawnerClient(conn), ctx, req)
	switch status.Code(err) {
	case codes.Unavailable:
		return nil, ErrNoRoute // the transport's readiness check is check-then-act
	case codes.DeadlineExceeded:
		return nil, ErrTimeout
	case codes.Canceled:
		return nil, ErrNodeDown
	}
	return resp, err
}

func peerErr(c wire.ErrorCode) error {
	if int(c) < 0 || int(c) >= len(peerErrs) {
		return fmt.Errorf("goactor: peer reported error code %d", c)
	}
	return peerErrs[c]
}

// spawner is the System's gRPC face, kept apart so that System exports no wire types.
type spawner struct {
	wire.UnimplementedSpawnerServer
	s *System
}

func (p spawner) Spawn(_ context.Context, r *wire.SpawnReq) (*wire.SpawnResp, error) {
	return p.s.handleSpawn(r), nil
}

func (p spawner) Lookup(_ context.Context, r *wire.LookupReq) (*wire.LookupResp, error) {
	return p.s.handleLookup(r), nil
}

func (s *System) handleSpawn(req *wire.SpawnReq) *wire.SpawnResp {
	id, created, err := s.spawnHere(req)
	code := slices.IndexFunc(peerErrs, func(e error) bool { return errors.Is(err, e) })
	return &wire.SpawnResp{Id: wire.FromIdent(id), Created: created, Error: wire.ErrorCode(code)}
}

func (s *System) spawnHere(req *wire.SpawnReq) (ActorID, bool, error) {
	e, ok := lookupBehavior(req.Behavior)
	if !ok {
		return ActorID{}, false, ErrUnknownBehavior
	}
	if req.ArgsType != codec.TypeName(e.argType) {
		return ActorID{}, false, ErrTypeMismatch
	}
	args, err := codec.Unmarshal(req.ArgsType, req.Args)
	if err != nil {
		return ActorID{}, false, ErrTypeMismatch
	}
	if req.Name == "" {
		id, err := e.spawn(s, args, "")
		return id, err == nil, err
	}

	msgType := codec.TypeName(e.msgType)
	s.namesMu.Lock()
	defer s.namesMu.Unlock()
	// Under namesMu: exit empties the table during Shutdown, and a live name must not read unbound.
	if s.ctx.Err() != nil {
		return ActorID{}, false, ErrNodeDown
	}
	if n, ok := s.names[req.Name]; ok {
		if n.msgType != msgType {
			return ActorID{}, false, ErrTypeMismatch
		}
		return n.id, false, nil
	}
	id, err := e.spawn(s, args, req.Name)
	if err != nil {
		return ActorID{}, false, err
	}
	s.names[req.Name] = nameEntry{id, msgType}
	return id, true, nil
}

func (s *System) handleLookup(req *wire.LookupReq) *wire.LookupResp {
	s.namesMu.Lock()
	defer s.namesMu.Unlock()
	if s.ctx.Err() != nil {
		return &wire.LookupResp{Error: wire.ErrorCode_NODE_DOWN}
	}
	n, ok := s.names[req.Name]
	if !ok {
		return &wire.LookupResp{Error: wire.ErrorCode_NAME_NOT_FOUND}
	}
	return &wire.LookupResp{Id: wire.FromIdent(n.id), MsgType: n.msgType}
}

// Registered before the goroutine starts: Start may send to itself, and a panicking Start must not
// race its own registration.
func spawnLocal[T any](s *System, a Actor[T], name string) (ActorID, error) {
	c := &ActorContext[T]{sys: s, name: name, actor: a, mb: mailbox.New[T](mailboxCap), quit: make(chan struct{})}
	s.mu.Lock()
	if s.ctx.Err() != nil {
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
	if c.name != "" {
		c.sys.unbind(c.name)
	}
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
