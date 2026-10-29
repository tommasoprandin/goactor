# goactor

This repository contains the software project for the Languages for Concurrency and Distribution
course of the Master Degree in Computer Science of the University of Padua.

The project is an actor framework for Go, providing typed actors, location-transparent messaging,
and distribution across a statically configured set of execution nodes.

## Contents

- [Scope](#scope)
- [Concepts](#concepts)
- [Configuration](#configuration)
- [Using the framework](#using-the-framework)
- [Mailboxes and delivery](#mailboxes-and-delivery)
- [Membership and liveness](#membership-and-liveness)
- [Naming](#naming)
- [Placement](#placement)
- [Failure model](#failure-model)
- [Transport](#transport)
- [Startup](#startup)
- [Repository layout](#repository-layout)
- [Dependencies](#dependencies)

## Scope

### Implemented

- Typed actors with compile-time checked message types (`Actor[T]`, `Ref[T]`).
- Actor spawning, local or remote, with automatic round-robin placement or explicit pinning.
- Location-transparent message delivery: identical semantics for local and remote actors.
- Global registration of actors under a name (a la Erlang _named actors_), with get-or-create
  semantics.
- Node membership and failure detection, disseminated by the SWIM protocol.
- Fast-fail delivery errors distinguishing a dead actor from an unreachable node.
- Timer-based self-messaging for application-level timeouts.

### Not implemented

- **Actor migration.** An actor lives on the node that created it until it terminates.
- **Load balancing.** Automatic placement is round-robin and reads no load statistics.
- **Supervision.** The framework reports actor crashes and never restarts, recreates or reparents
  an actor.
- **Monitors or links.** Peer death is observed through send errors and application-level timeouts.
- **Deadlock or liveness detection** of actors.
- **Dynamic cluster membership.** The node set is fixed at startup by the configuration file.
- **Mailbox backpressure.** Mailboxes are unbounded (as Erlang).
- **Cross-language interoperability.** The wire format is Go-specific (gob).
- **Transport security.** Inter-node traffic is plaintext.

## Concepts

### Execution node

One process running the framework. It manages transport to every other node, actor registration,
local scheduling, message routing, and the table of named actors it hosts.

A process runs at most one `System` at a time, and `goactor.Start` fails while another is running.
A multi-node cluster is therefore always several processes, on one host or many.

All nodes run the **same binary** with an identical configuration file; a node's own identity comes
from a command-line flag.

### Actor

A goroutine, a mailbox, and private state. An actor processes one message at a time, so its state
requires no synchronisation. Actors are addressed only through references and never share memory.

### Identity

```go
type NodeID uint16

type ActorID struct {
    Node  NodeID  // logical node id, from configuration
    Epoch uint32  // node incarnation
    Seq   uint64  // node-local actor counter
}
```

`ActorID` is comparable and usable as a map key.

- `Node` routes a message to its host without a lookup.
- `Epoch` distinguishes runs of a node. It is incremented from a counter file in the data directory
  at every startup, so actors of an old incarnation read as dead.
- `Seq` is unique for every pair (node ID, epoch).

### Behavior

A named constructor, registered at package initialisation and compiled into the binary:

```go
var (
    WorkerBehavior = goactor.Register("mandelbrot.Worker", func(_ struct{}) goactor.Actor[WorkerMsg] {
        return &Worker{}
    })
    CoordinatorBehavior = goactor.Register("mandelbrot.Coordinator", func(p Params) goactor.Actor[CoordMsg] {
        return &Coordinator{p: p}
    })
)
```

`Register` returns a `Behavior[A, T]` handle carrying the argument and message types, so `Spawn` and
`SpawnNamed` reject mismatched `args` and references at compile time, and infer `A` and `T`. It also
registers both types with the wire codec.

The handle is compile-time only: on the wire a behavior is just its name, resolved in the target
node's registry. Every node runs the same binary, so every node can construct every behavior, and a
remote spawn transmits only the name and the serialised arguments. **Code is never transmitted.**

Behaviors are declared in Go code only, never in the configuration file.

## Configuration

A single YAML file, identical on every node, describes the cluster topology:

```yaml
cluster:
  - { id: 1, gossip: "10.0.0.1:7946", grpc: "10.0.0.1:9000" }
  - { id: 2, gossip: "10.0.0.2:7946", grpc: "10.0.0.2:9000" }
  - { id: 3, gossip: "10.0.0.3:7946", grpc: "10.0.0.3:9000" }
data_dir: /var/lib/goactor

gossip:
  probe_interval:  1s
  probe_timeout:   500ms
  indirect_checks: 3
  suspicion_mult:  4
```

```
myapp -config cluster.yaml -id 2
```

`gossip` carries SWIM membership traffic and `grpc` carries actor messages; the two planes may be
reachable independently of one another.

The `gossip` block is optional; an omitted key takes the default shown above. It trades detection
latency against probe traffic and the risk of declaring a slow node dead.

## Using the framework

### Defining an actor

```go
// An actor has one message type. One that receives several kinds of message
// takes a struct with one field per kind, and acts on the field that is set.
type WorkerMsg struct {
    Tile *Tile
    Ping bool
}

type Tile struct {
    Index   int
    Rect    image.Rectangle
    Size    int
    MaxIter int
    ReplyTo goactor.Ref[CoordMsg]
}

type CoordMsg struct {
    Hello *Hello
    Done  *TileDone
    Begin bool
    Tick  bool
}

type TileDone struct {
    Index  int
    Pixels []byte
    Worker goactor.Ref[WorkerMsg]
}

type Worker struct{}

// Lifecycle hooks. This worker holds no resources, so both are empty.
func (w *Worker) Start(ctx *goactor.ActorContext[WorkerMsg]) {}
func (w *Worker) Stop(ctx *goactor.ActorContext[WorkerMsg], err error) {}

// The actual worker body that takes in
// its context and the typed message
func (w *Worker) Receive(ctx *goactor.ActorContext[WorkerMsg], m WorkerMsg) {
    if m.Tile == nil {
        return
    }
    done := TileDone{Index: m.Tile.Index, Pixels: renderTile(*m.Tile), Worker: ctx.SelfRef()}
    m.Tile.ReplyTo.Send(CoordMsg{Done: &done})
}
```

Every actor implements three methods. `Start` and `Stop` run on the actor's goroutine, outside the
mailbox loop, so they never interleave with `Receive`:

```go
type Actor[T any] interface {
    Start(ctx *goactor.ActorContext[T])
    Receive(ctx *goactor.ActorContext[T], msg T)
    Stop(ctx *goactor.ActorContext[T], err error)
}
```

`Start` runs once, before the first message is dequeued, and is where initialisation happens.
Initialisation that cannot succeed must panic: the actor is reported dead and the first `Send` to it
returns `ErrActorDead`. `Spawn` does not wait for `Start`, so it never reports the failure.

`Stop` runs once for every actor whose `Start` returned, on shutdown and on panic alike. `err` is
`nil` on a normal stop and wraps the recovered value on a panic, in which case it runs before the
mailbox is discarded. An actor that panics inside `Start` never receives `Stop`. A panic inside
`Stop` is recovered and logged.

An actor cannot terminate itself: it lives until it panics or its node goes away. `Shutdown` stops
every actor of the node, waits a bounded time for them to exit, and makes any later `Spawn` on that
node fail with `ErrNodeDown`.

### API

Go does not permit type parameters on methods, so operations that introduce a new message type are
package-level functions taking a context.

```go
// Registration (package init time)
func Register[A, T any](name string, ctor func(A) Actor[T]) Behavior[A, T]
func RegisterMessage[T any](name string)

// Spawning
func Spawn[A, T any](ctx Context, b Behavior[A, T], args A, opts ...SpawnOpt) (Ref[T], error)
func SpawnNamed[A, T any](ctx Context, name string, b Behavior[A, T], args A) (Ref[T], bool, error)
func Lookup[T any](ctx Context, name string) (Ref[T], error)

// Messaging
func (r Ref[T]) Send(msg T) error
func Request[T, R any](ctx Context, r Ref[T], msg T, timeout time.Duration) (R, error)

// Context
func (c *ActorContext[T]) Self() ActorID
func (c *ActorContext[T]) SelfRef() Ref[T]
func (c *ActorContext[T]) After(d time.Duration, msg T) CancelFunc

// Spawn options
func WithNode(id NodeID) SpawnOpt
```

`SpawnNamed` is get-or-create: every node may call it with the same name, exactly one actor is
created, and all callers receive a reference to it. The actor always runs on the name's owner, so
it takes no placement options; see [Naming](#naming). The boolean reports whether this call created
it.

`Lookup[T]` fails unless the named actor accepts messages of type `T`.

`Request` blocks the calling actor's goroutine until a reply arrives or the timeout expires. Request
cycles between actors deadlock; use `Send` with `After` for scatter-gather. The request type `T`
must be a struct with a field `ReplyTo Ref[R]`, which `Request` sets to a single-use reply address;
the responder replies with `ReplyTo.Send`.

`Ref[T]` holds only the target's `ActorID`, so it may be carried in a message as a reply address.
`Send` takes no context and goes through the `System` running in the process; with none running, it
fails with `ErrSystemNotStarted`.

### Bootstrapping an application

`main` is identical on every node and contains no branch on node identity. The singleton is
established by its name, which fixes the one node that hosts it:

```go
func main() {
    cfg, err := config.Load(*cfgPath)
    if err != nil {
        log.Fatal(err)
    }
    sys, err := goactor.Start(cfg, *nodeID)
    if err != nil {
        log.Fatal(err)
    }
    defer sys.Shutdown()

    coordinator, created, err := goactor.SpawnNamed(sys.Root(), "coordinator", CoordinatorBehavior,
        Params{Size: 4096, Tiles: 256, Workers: 6, MaxIter: 10000, Nodes: cfg.N()})
    if err != nil {
        log.Fatal(err)
    }
    if created {
        log.Print("this call created the coordinator")
    }
    if err := coordinator.Send(CoordMsg{Hello: &Hello{Node: *nodeID}}); err != nil {
        log.Fatal(err)
    }

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)
    <-sig
}
```

The coordinator then spawns workers with `Spawn`. Nodes do not wait for each other at startup, so an
application that wants its actors spread across the whole cluster waits itself: here, for a `Hello`
from every node.

## Mailboxes and delivery

Mailboxes are **unbounded**: `Send` never blocks, and a slow consumer accumulates messages until the
node exhausts memory.

Each mailbox holds an outer queue appended to by senders under a mutex, and an inner queue owned by
the actor goroutine. A drained inner queue is refilled by a single buffer swap, giving one lock
acquisition per batch rather than per message.

Guarantees:

- **At-most-once delivery.** Messages are not retransmitted.
- **FIFO per sender-receiver pair**, within a connection epoch.
- **Copy semantics.** All messages are serialised, including local sends, so behaviour does not
  change with placement.

Messages are encoded individually with `encoding/gob` against a type-name registry, so every payload
is self-describing. `Register` registers a behavior's types under their `pkgpath.TypeName`; a type
used only as a `Request` reply is registered with `RegisterMessage`. Sending an unregistered type
fails with `ErrUnknownType`.

Unexported fields are not transmitted. Channel and function fields fail at encode with an error
naming the field, as does a Go-embedded field whose type has its own encoder (`Ref[T]`, `ActorID`,
`time.Time`): gob would promote that encoder to the whole message and silently drop every other
field. Carry such values in named fields.

Local and remote sends share one path: a local envelope goes straight to the delivery routine the
transport calls for inbound traffic. Only the header's byte framing is skipped locally.

## Membership and liveness

The node set is fixed, so membership only keeps track of which peers are running. 
To achieve this the framework uses the [SWIM protocol](https://dl.acm.org/doi/10.1145/383962.384010]).
Traffic related to that 

**Detection.** Every `probe_interval`, each node pings one random peer. With no ack within
`probe_timeout`, it asks `indirect_checks` other peers to ping the target, so a dropped packet or
one-way fault is not mistaken for death. If all fail the target becomes **suspect**, and the opinion
is gossiped. A live node refutes it; otherwise it becomes **dead** when the suspicion timeout
expires. That timeout shrinks as independent suspicions accumulate, and is roughly
`suspicion_mult * log10(N+1) * probe_interval`, on top of the round or two needed to notice the
missing ack.

**Rejoining.** SWIM never probes a peer it believes down, so every `probe_interval` each node also
contacts every configured peer that is not alive. This covers the first node up, peers down at
startup, and healed partitions, where a node wrongly declared dead hears the accusation and refutes
it.

**Epochs.** Each node gossips a fixed-width record of its id and epoch, written once at join. A node
returning with the same epoch was merely slow and references to it stay good; a higher epoch is a
new process, and its old actors are gone.

**The local view** maps node id to state and epoch, read without a network call:

- `Send` to a remote actor fails with `ErrNodeDown` if its node is not alive, before touching the
  transport.
- `Spawn` skips nodes that are not alive.
- An epoch change invalidates references to the previous incarnation.
- `SpawnNamed` and `Lookup` report `ErrRegistryUnavailable` while a name's owner is not alive.

**Liveness is not reachability.** SWIM and the gRPC streams are independent signals that can
disagree. Membership is consulted first: a peer alive but with no stream yields `ErrNoRoute`, and
one gossiped dead yields `ErrNodeDown` even over a working stream. Neither is retried.

Consequences of an eventually consistent view:

- A dead node is believed alive until detected; sends to it are accepted and lost, which
  at-most-once delivery permits.
- A live node can be wrongly declared dead by a long GC pause or brief partition. Sends fast-fail,
  placement skips it and its names are unavailable until it returns. Nothing needs a leader or
  quorum, so a divergent view wastes work but cannot corrupt state.
- Membership never changes name ownership, so no failure triggers rehashing.

`Shutdown` leaves the cluster gracefully, so peers record the departure at once.

## Naming

A name's owner hosts its actor. The owner is the node at index `fnv1a(name) % N` of the configured
cluster list, so ownership is fixed: no rehashing, rebalancing or migration.

There is no registry service. Each node holds a table of the named actors it hosts,
`name -> (ActorID, message type name)`, reached directly on the owner and over RPC from elsewhere.
`SpawnNamed` holds the table's lock across check, construction and insert, giving a single winner: a
caller that finds the name bound constructs nothing, and fails with `ErrTypeMismatch` if it is bound
to a different message type.

Both functions return an ordinary `Ref`, so later sends never touch the table and callers cache
nothing.

An entry is removed before its actor leaves the node's actor table, so a name never resolves to an
actor known to be dead and a later `SpawnNamed` constructs a new one. A restarted owner's table is
correctly empty: its names stay unbound until something calls `SpawnNamed`. No other node can take
over a name, which rules out duplicates even under partition.

While a name's owner is down or unreachable, `Lookup` and `SpawnNamed` report
`ErrRegistryUnavailable`, distinct from `ErrNameNotFound`. `Lookup` reports it at once. `SpawnNamed`
retries every 250 ms for up to 30 s, stopping with `ErrNodeDown` if the local `System` stops first,
because `main` routinely calls it before the owner is up. The name makes a retry idempotent. If a
response is lost, the retry reports `false` for the call that did create the actor, so possibly no
caller sees `true`; this is an accepted limitation.

## Placement

`Spawn` without options is round-robin, and does not mean local: each node keeps a counter over the
configured cluster list, starting at a random position, and takes the next node alive in its view.
With no node alive, it fails with `ErrNoLiveNodes`. `WithNode(id)` pins the actor, failing if that
node is not alive.

Placement reads no load statistics: a burst of spawns is over before gossip could report its effect.
The spread is even only across the nodes alive during the burst, and a restarted node gets no more
than its turn.

## Transport

gRPC over HTTP/2, plaintext, in a full mesh of automatically re-established connections. Each
connection carries two kinds of traffic:

- **Actor messages** travel as opaque envelopes on one long-lived bidirectional stream per ordered
  node pair, with batching. The stream provides FIFO per pair, and batching spreads gRPC's per-call
  cost over small, numerous, one-way messages.
- **Framework requests** (remote spawn, name lookup) are typed unary RPCs generated from protobuf,
  so correlation, deadlines and cancellation come from gRPC, and they never queue behind actor
  traffic. Behaviour arguments stay gob-encoded.

Errors a peer reports travel in the response body and map onto the sentinel errors of the
[failure model](#failure-model); gRPC status carries transport failures only. gRPC retries no call,
so each request reaches its peer at most once.

## Startup

1. Load the configuration and resolve the local node identity from `-id`. Fail if this process
   already runs a `System`.
2. Read and increment the epoch counter in the data directory.
3. Open transport streams to all configured peers.
4. Join the SWIM cluster, and begin publishing liveness and the epoch.
5. Run application code, which spawns actors.

Nodes do not wait for the full cluster to become available before starting.

## Repository layout

```
goactor/
  actor.go            Actor[T], ActorContext[T], Ref[T]
  id.go               ActorID, NodeID re-exports, sequence allocator, epoch file
  system.go           node runtime, spawn, placement, named actors, dispatch, panic recovery
  registry.go         behavior name -> constructor
  mailbox/            two-queue unbounded mailbox
  codec/              gob type registry, envelopes
  transport/          gRPC connections, per-pair message streams
  cluster/            memberlist integration, epochs
  config/             YAML parsing and validation
  internal/ident/     ActorID, NodeID definitions, shared by every subpackage
  internal/wire/      protobuf definitions of the framework RPCs
  examples/           pingpong_local (single node), mandelbrot (cluster demo)
```

## Dependencies

- `github.com/hashicorp/memberlist` — SWIM membership and failure detection
- `google.golang.org/grpc` — inter-node transport
- `google.golang.org/protobuf` — generated framework RPC messages
- `go.yaml.in/yaml/v3` — configuration
