# goactor

This repository contains the software project for the Languages for Concurrency and Distribution course of the Master Degree in Computer Science of the University of Padua.

The project is an actor framework for Go, providing typed actors, location-transparent messaging, and distribution
across a statically configured set of execution nodes.


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
- Global registration of actors under a name (a la Erlang _named actors_), with get-or-create semantics.
- Node membership and failure detection, disseminated by the SWIM protocol.
- Fast-fail delivery errors distinguishing a dead actor from an unreachable node.
- Timer-based self-messaging for application-level timeouts.

### Not implemented

- **Actor migration.** An actor lives on the node that created it until it terminates.
- **Load balancing.** Automatic placement is round-robin and reads no load statistics.
- **Supervision.** The framework never restarts, recreates, or reparents an actor. It simply reports actor crashes; deciding what to do about them is application logic.
- **Monitors or links.** There is no `DOWN` message and no failure subscription. Peer death is observed through send errors and application-level timeouts.
- **Deadlock or liveness detection** of actors.
- **Dynamic cluster membership.** The node set is fixed at startup by the provided configuration file.
- **Mailbox backpressure.** Mailboxes are unbounded (as Erlang).
- **Cross-language interoperability.** The wire format is Go-specific (gob).
- **Transport security.** Inter-node traffic is plaintext; TLS is out of scope.

## Concepts

### Execution node

One process running the framework. Each node manages transport to every other node, actor
registration, local actor scheduling, message routing, and the table of named actors it hosts.

A process hosts exactly one node: it runs at most one `System` at a time, and `goactor.Start` fails
while another is running. A multi-node cluster is therefore always several processes, on one host
or many.

All nodes run the **same binary**. The node set is fixed at startup by a configuration file
identical on every node; a node's own identity is supplied by a command-line flag.

### Actor

A goroutine, a mailbox, and private state. An actor processes one message at a time, so its state
requires no synchronisation. Actors are addressed exclusively through references and never share
memory.

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

`Node` field allows the execution node to route messages to local nodes without having to do an explicit lookup.

`Seq` distinguishes actors within the same execution node. It must be unique for every pair (node ID, epoch).

`Epoch` distinguishes runs of the node process
itself, and is incremented from a counter file in the data directory at every startup. This is used to invalidate messages delivered to actors in an "old" node instance, forcing the sender to consider it as dead.

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

The constructor's argument and message types are part of the behavior's contract: `Register` returns
a `Behavior[A, T]` handle carrying both, so `Spawn` and `SpawnNamed` reject mismatched `args` and
references at compile time, and infer `A` and `T`. The handle is compile-time only: on the wire a
behavior is just its name, and a node resolves that name in its own registry. `Register` also
registers both types with the wire codec.

The code of every actor is replicated in every execution node (the binary is the same), so every node can construct every behavior, which is what allows an actor to be placed on any node.

Then, to spawn an actor on a remote node it is sufficient to transmit a behavior name and its serialized arguments; **code is never transmitted**, since Go does not support serialization of code (excluding plugins).

Behaviors are declared in Go code only. The configuration file contains no actor or behavior information.

## Configuration

Configuration is written is `yaml` format on a single file, which should be provided identically to node. It describes the cluster topology:

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

Each node needs two addresses. `gossip` carries SWIM membership traffic and `grpc` carries actor
messages; they are separate planes and may be reachable independently of one another.

The `gossip` block is optional, and omitting it — or any single key within it — applies the default
shown above. It tunes failure detection, trading detection latency against probe traffic and against
the risk of declaring a merely slow node dead; see
[Membership and liveness](#membership-and-liveness). Like every other key, it is cluster-wide and
must be identical on every node.

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

Every actor implements three methods. `Start` and `Stop` are invoked by the runtime on the actor's
goroutine, outside the mailbox loop, so they never interleave with `Receive`:

```go
type Actor[T any] interface {
    Start(ctx *goactor.ActorContext[T])
    Receive(ctx *goactor.ActorContext[T], msg T)
    Stop(ctx *goactor.ActorContext[T], err error)
}
```

`Start` runs once, before the first message is dequeued. It is the first point at which `ActorContext[T]` exists and where any kind of initialization happens, so for instance opening file handles or spawning childrens. Initialisation that cannot succeed must panic: the actor is then reported dead, and the first `Send` to its reference returns `ErrActorDead`. `Spawn` itself does not wait for `Start` and so never reports the failure.

`Stop` runs once for every actor whose `Start` returned normally — on shutdown and on the panic path alike. `err` is `nil` when the actor stopped normally and wraps the recovered value when it panicked. An actor that panics inside `Start` never receives `Stop`, as its state is only partly built.
A panic inside `Stop` is recovered and logged.

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
`SpawnNamed` takes no placement options; see [Naming](#naming). The boolean reports whether this
call created it.

`Lookup[T]` verifies that the named actor accepts messages of type `T` and fails otherwise.

`Request` blocks the calling actor's goroutine until a reply arrives or the timeout expires.
Request cycles between actors deadlock; use `Send` with `After` for scatter-gather. The request
type `T` must be a struct with a field `ReplyTo Ref[R]`, which `Request` sets to a single-use reply
address; the responder replies with `ReplyTo.Send`.

`Ref[T]` is serializable and may be carried in a message field as a reply address. It holds only
the target's `ActorID`, which is all that is serialised; `Send` takes no context and goes through
the `System` running in the process. Used with no `System` running, it fails with
`ErrSystemNotStarted`.

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

The coordinator then spawns workers with `Spawn`, which places them round-robin across the alive
nodes. Nodes do not wait for each other at startup, so an application that wants its actors spread
across the whole cluster waits for that itself: here, for a `Hello` from every node.

## Mailboxes and delivery

Mailboxes are **unbounded**. `Send` never blocks and never applies backpressure. A slow consumer
accumulates messages until the node exhausts memory.

Each mailbox holds two queues: an outer queue appended to by senders under a mutex, and an inner
queue owned exclusively by the actor goroutine. The inner queue is refilled from the outer queue by
a single buffer swap when it drains, giving one lock acquisition per batch rather than per message.

Guarantees:

- **At-most-once delivery.** Messages are not retransmitted.
- **FIFO per sender-receiver pair**, within a connection epoch.
- **Copy semantics.** All messages are serialized, including local sends. Actors never share memory,
  and behaviour does not change with placement.

Messages are encoded with `encoding/gob` against a type-name registry. `Register` registers a
behavior's argument and message types under their `pkgpath.TypeName`; a type used only as a
`Request` reply is registered with `RegisterMessage`. Each message is encoded on its own, so every
payload is self-describing and independent of any stream state. Sending an unregistered type fails
with `ErrUnknownType`.

Message types must be encodable. Unexported fields are not transmitted. Channel and function fields
fail at encode with an error naming the field, as does a Go-embedded field whose type has its own
encoder (`Ref[T]`, `ActorID`, `time.Time`): gob would promote that encoder to the whole message and
silently drop every other field. Carry such values in named fields.

Local and remote sends share one path. The sender encodes the message into an envelope; a local
envelope is handed straight to the same delivery routine the transport calls for inbound traffic,
which looks up the target, decodes the payload and enqueues it. Only the envelope header's byte
framing is skipped locally, and it carries no user data.

## Membership and liveness

The node set is fixed by configuration, so membership is never used for discovery — every node
knows every peer's addresses before it starts. What configuration cannot state is which of those
peers are running at any given moment. SWIM supplies that as the framework's failure detector. It
runs over the `gossip` address, on a plane separate from the gRPC transport.

The alternatives were rejected on cost. All-to-all heartbeating needs O(N^2) messages per round,
and tightening its detection time only makes that worse; a central monitor is a single point of
failure that would need its own failover, which is more machinery than the rest of this design
contains. SWIM keeps the work each node does per round constant and independent of N, and reuses
the probe traffic to carry state changes, so dissemination costs no messages of its own. The price
is that the view is eventually consistent, and every consumer must tolerate reading it slightly
wrong.

**The round.** Every `probe_interval`, each node picks one peer at random and pings it. If the ack
does not arrive within `probe_timeout`, the node asks `indirect_checks` other peers to ping that
same target on its behalf. This indirect probe is what stops a dropped packet or a one-way fault
from being mistaken for a dead node: the target is doubted only when several independent paths to it
fail. If none of them succeed, the target is marked **suspect** and that opinion is gossiped onward.
A suspected node that is in fact alive hears the accusation and refutes it, which restores it
everywhere; one that does not refute is promoted to **dead** when the suspicion timeout expires.
That timeout shrinks as independent suspicions accumulate, so a genuinely dead node is confirmed
faster than a merely slow one.

Detection is therefore neither instantaneous nor fixed — its latency follows from configuration.
The window before a dead node is declared dead grows roughly as
`suspicion_mult * log10(N+1) * probe_interval`, on top of the round or two needed to notice the
missing ack.

**Rejoining.** SWIM only probes nodes it believes alive, so the protocol alone never reaches a peer
that was down at startup or has been declared dead. Every `probe_interval`, each node therefore
contacts every configured peer that is not alive. The same loop covers the first node up, which
finds nobody, and a healed partition, whose sides have stopped probing each other: on contact, a
node wrongly declared dead hears the accusation and refutes it.

**What each node publishes.** Alongside its liveness state, every node attaches a small record to
its gossip traffic: its node id and its current epoch. That record has to fit inside a bounded
metadata allowance, so it is fixed-width integers throughout and carries no strings. Neither field
changes while a process runs, so the record is written once at join and never refreshed.

Carrying the epoch here is what lets a restart be told from a recovery. A node that was unreachable
and returns with the epoch it had before was merely slow, and references to its actors are still
good; one that returns with a higher epoch is a new process, and every actor the cluster remembers
on it is gone.

**The local view.** Each node folds all of this into a table of node id to state and epoch, read
without a network call. It is consulted on every path that needs to know something about another
node:

- `Ref[T].Send` to a remote actor checks liveness first, and fails with `ErrNodeDown` before
  touching the transport.
- `Spawn` skips nodes that are not alive when choosing a host for a new actor.
- An epoch change invalidates cached references to the previous incarnation.
- `SpawnNamed` and `Lookup` report `ErrRegistryUnavailable` while a name's owner is not alive.

**Liveness is not reachability.** SWIM's opinion and the state of the gRPC streams are independent
signals gathered on different ports, and they can disagree. A peer may be alive in the membership
view while the stream to it is still being re-established, or be gossiped as dead while a working
stream to it remains open. Membership is consulted first, so the former yields `ErrNoRoute` and the
latter `ErrNodeDown`. Neither is retried: the framework reports, and the application decides.

Consequences of an eventually consistent view:

- A node that dies is believed alive until the detection window elapses. Sends to it in that period
  are accepted locally and then lost, which at-most-once delivery already permits.
- A live node can be wrongly declared dead — a long garbage collection pause or a brief partition
  is enough. The cost is bounded and self-correcting: sends to it fast-fail, placement skips it,
  and its names are unavailable until it is alive again. Nothing in the design elects a leader or
  needs a quorum, so a divergent view cannot corrupt state, only waste work.
- Membership never changes name ownership. A name's owner is taken over the configured node set
  rather than the alive one, so a dead node's names stay its names and no failure triggers
  rehashing.

`Shutdown` leaves the cluster gracefully, so peers record the departure immediately instead of
waiting out a suspicion timeout. A node killed outright is detected on the ordinary path above.

## Naming

A name's owner hosts its actor. The owner is the node at index `fnv1a(name) % N` of the configured
cluster list. That list never changes, so ownership is fixed and there is no rehashing, rebalancing
or migration.

There is no registry service. Each node holds a table of the named actors it hosts, mapping
`name -> (ActorID, message type name)`. `SpawnNamed` and `Lookup` use the owner's table, directly on
the owner and over RPC from any other node. `SpawnNamed` holds the table's lock across check,
construction and insert, which gives a single winner and no losing candidates: a caller that finds
the name bound constructs nothing, and fails with `ErrTypeMismatch` if it is bound to a different
message type.

Both functions return an ordinary `Ref`, so later sends never touch the table. Callers cache
nothing: a `Ref` already survives anything its actor survives.

When a named actor terminates, the framework removes its entry before the actor leaves the node's
actor table. An actor that dies at once therefore leaves no stale entry, a name never resolves to an
actor already known to be dead, and a subsequent `SpawnNamed` constructs a new actor.

A named actor dies with its host, and its entry with it. A restarted owner's empty table is
therefore correct, and there is nothing to rebuild or re-announce: its names are unbound when it
returns, until something calls `SpawnNamed`. No other node can take over a name, which is what rules
out duplicates even under partition. An owner wrongly declared dead loses nothing: its names resolve
unchanged once it is alive again.

Availability:

- While a name's owner is down or unreachable, `Lookup` and `SpawnNamed` report
  `ErrRegistryUnavailable`, which is distinct from `ErrNameNotFound`. Names owned by other nodes are
  unaffected.
- `Lookup` reports it at once. `SpawnNamed` retries every 250 ms for up to 30 s, stopping with
  `ErrNodeDown` if the local `System` stops first: nodes do not wait for each other at startup, so
  `main` routinely calls it before the owner is up. The name makes the call idempotent, so a retry
  cannot create a second actor.
- If a response is lost, the retry reports `false` for the call that did create the actor, so
  possibly no caller sees `true`. This is an accepted limitation.

## Placement

`Spawn` without options places actors round-robin. Each node keeps its own counter over the
configured cluster list and takes the next node that is alive in its membership view, so dead nodes
are skipped without weighting the cycle. The calling node takes its turn like any other, so `Spawn`
without options does not mean local. Counters start at a random position, so that nodes do not all
send their first spawn to the same target. With no node alive, `Spawn` fails with `ErrNoLiveNodes`.

Placement reads no load statistics. The case that matters is a burst of spawns, which should be over
before gossip could report its effect, so statistics would be flat exactly when they were needed.
Round-robin spreads that burst evenly. Its limitations are accepted: the spread is even only across
the nodes alive during the burst, and a node that restarts empty gets no more than its turn.

`Spawn` with `WithNode(id)` pins the actor to a specific node, failing if that node is not alive.

## Failure model

The framework detects and reports failures. It does not act on them.

An actor is considered dead when it panics (the panic is recovered and the actor terminated) or when
its node shuts down; actors never terminate themselves. A panicking actor's `Stop` is invoked with
the recovered value before its mailbox is discarded; an actor that panics in `Start` is dead at once
and `Stop` is not called. Fatal Go runtime errors — concurrent map writes, stack exhaustion,
all-goroutines-asleep — terminate the entire process and are handled as a node failure.

Node failures are detected by SWIM, after the detection window described in
[Membership and liveness](#membership-and-liveness); sends issued before the failure is noticed are
lost rather than refused. A failed node's actors are marked dead and no new actors are placed on
it. On rejoin, the node takes a fresh epoch, so references held from before the failure
remain dead rather than addressing newly created actors.

Sentinel errors returned by the API:

| Error | Meaning | Cost |
| --- | --- | --- |
| `ErrActorDead` | Destination node is up; the actor is gone or the epoch is stale | one round trip |
| `ErrNodeDown` | Destination node is not alive in the local membership view, or a spawn after the local `Shutdown` | none, fails locally |
| `ErrNoRoute` | No transport connection to the destination node | none, fails locally |
| `ErrUnknownNode` | Destination `NodeID` is absent from the configuration | none, fails locally |
| `ErrNameNotFound` | Lookup of an unregistered or released name | one call to the owner |
| `ErrTypeMismatch` | `Lookup[T]` or `SpawnNamed` found a different message type, `Spawn` args do not match the target node's behavior, or a `Request` type lacks `ReplyTo Ref[R]` | one call to the owner or target, or none |
| `ErrRegistryUnavailable` | The owner of this name is not reachable | at most one call to the owner, per attempt; `SpawnNamed` retries for up to 30 s |
| `ErrUnknownBehavior` | Target node has no behavior registered under that name | one round trip |
| `ErrUnknownType` | Message type is not registered with the codec | none, fails locally |
| `ErrNoLiveNodes` | Automatic placement found no alive node | none, fails locally |
| `ErrSystemNotStarted` | A `Ref[T]` was used with no `System` running in this process | none, fails locally |
| `ErrTimeout` | `Request` or remote `Spawn` deadline expired; a spawn may still have taken effect | the timeout |

`ErrUnknownType` fails at local encode. The receiver-side check that answers with a `Nack` carrying
`ErrUnknownType` exists only as defence against mismatched binaries; such a `Nack` resolves a
pending `Request` or is dropped.

Consequences applications must handle:

- An actor that only sends and never expects replies receives no indication that its peer has died.
- A timeout does not distinguish a slow actor from a dead one. Retrying may duplicate work.

## Transport

gRPC over HTTP/2. Connections form a full mesh and are re-established automatically. Traffic is
plaintext; transport security is out of scope. Each connection carries two kinds of traffic:

- **Actor messages** travel as envelopes on one long-lived bidirectional stream per ordered node
  pair, with batching. The stream is what provides FIFO per pair, and batching spreads the per-call
  cost of gRPC over messages that are small, numerous and one-way. Payloads are opaque to gRPC:
  message types belong to the application, so no code generated with the framework can name them.
- **Framework requests** — remote spawn and name lookup — are typed unary RPCs generated from
  protobuf. They are genuinely request/response and need no ordering against actor
  traffic, so correlation, deadlines and cancellation come from gRPC rather than from the
  framework, and they never queue behind a batch of actor messages. Behaviour arguments in a spawn
  request stay gob-encoded, for the same reason as message payloads.

Errors a peer reports travel in the response body and map onto the sentinel errors of the
[failure model](#failure-model); gRPC status carries transport failures only. gRPC retries no call,
so each request reaches its peer at most once. `SpawnNamed` retries at application level, as a new
request, which its name makes idempotent.

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
cmd/mandelbrot/       demo application
examples/             minimal programs showing library usage (pingpong_local: single node)
```

## Dependencies

- `github.com/hashicorp/memberlist` — SWIM membership and failure detection
- `google.golang.org/grpc` — inter-node transport
- `google.golang.org/protobuf` — generated framework RPC messages
- `go.yaml.in/yaml/v3` — configuration
