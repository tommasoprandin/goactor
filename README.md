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
- Actor spawning, local or remote, with automatic placement (supporting load balancing) or explicit pinning.
- Location-transparent message delivery: identical semantics for local and remote actors.
- Global registration of actors under a name (a la Erlang _named actors_), with get-or-create semantics.
- Node membership, failure detection, and load statistics, disseminated by the SWIM protocol.
- Fast-fail delivery errors distinguishing a dead actor from an unreachable node.
- Timer-based self-messaging for application-level timeouts.

### Not implemented

- **Actor migration.** An actor lives on the node that created it until it terminates.
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
registration, local actor scheduling, message routing, and its shard of the name registry.

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
func init() {
    goactor.Register("mandelbrot.Worker", func(_ struct{}) goactor.Actor[Tile] { return &Worker{} })
    goactor.Register("mandelbrot.Coordinator", func(p Params) goactor.Actor[TileDone] {
        return &Coordinator{params: p}
    })
}
```

The constructor's argument and message types are part of the behavior's contract: `Spawn` and
`SpawnNamed` type-check their `args` against the argument type and their type parameter `T` against
the message type, and fail with `ErrTypeMismatch` on disagreement. `Register` registers both types
with the wire codec.

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
  meta_refresh:    1s

placement:
  w_actors: 0.2
  w_depth:  0.5
  w_cpu:    0.3
  k_actors: 100
  k_depth:  8
```

```
myapp -config cluster.yaml -id 2
```

Each node needs two addresses. `gossip` carries SWIM membership traffic and `grpc` carries actor
messages; they are separate planes and may be reachable independently of one another.

The `gossip` and `placement` blocks are optional, and omitting a block — or any single key within
one — applies the default shown above. `gossip` tunes failure detection, trading detection latency
against probe traffic and against the risk of declaring a merely slow node dead; see
[Membership and liveness](#membership-and-liveness). `placement` weights the load score used to
choose a host for a new actor; see [Placement](#placement). Like every other key, both blocks are
cluster-wide and must be identical on every node.

## Using the framework

### Defining an actor

```go
// The actor internal state
type Tile struct {
    Index   int
    Y0, Y1  int
    ReplyTo goactor.Ref[TileDone]
}

// Message types
type TileDone struct {
    Index  int
    Pixels []byte
    Worker goactor.Ref[Tile]
}

type Worker struct{}

// Lifecycle hooks. This worker holds no resources, so both are empty.
func (w *Worker) Start(ctx *goactor.ActorContext[Tile]) {}
func (w *Worker) Stop(ctx *goactor.ActorContext[Tile], err error) {}

// The actual worker body that takes in
// its context and the typed message
func (w *Worker) Receive(ctx *goactor.ActorContext[Tile], t Tile) {
    t.ReplyTo.Send(TileDone{
        Index:  t.Index,
        Pixels: render(t.Y0, t.Y1),
        Worker: ctx.SelfRef(),
    })
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
func Register[A, T any](name string, ctor func(A) Actor[T])
func RegisterMessage[T any](name string)

// Spawning
func Spawn[T any](ctx Context, behavior string, args any, opts ...SpawnOpt) (Ref[T], error)
func SpawnNamed[T any](ctx Context, name, behavior string, args any) (Ref[T], bool, error)
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

`SpawnNamed` is get-or-create: every node may call it with the same name, exactly one registration
wins, and all callers receive a reference to the winning actor. The candidate is always constructed
on the calling node — the creator hosts it, so `SpawnNamed` takes no placement options. A losing
candidate is constructed and then stopped before its reference escapes; behaviors used with
`SpawnNamed` must therefore tolerate `Start` and `Stop` running for a candidate that never serves.
The boolean reports whether this call won.

`Lookup[T]` verifies that the registered actor accepts messages of type `T` and fails otherwise.

`Request` blocks the calling actor's goroutine until a reply arrives or the timeout expires.
Request cycles between actors deadlock; use `Send` with `After` for scatter-gather. The request
type `T` must be a struct with a field `ReplyTo Ref[R]`, which `Request` sets to a single-use reply
address; the responder replies with `ReplyTo.Send`.

`Ref[T]` is serializable and may be embedded in messages as a reply address. Besides the target's
`ActorID` it holds the local `System` it sends through, since `Send` takes no context; only the
`ActorID` is serialised.

### Bootstrapping an application

`main` is identical on every node and contains no branch on node identity. The singleton is
established by the name registry:

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

    _, created, err := goactor.SpawnNamed[TileDone](sys.Root(),
        "coordinator", "mandelbrot.Coordinator", Params{Width: 4096, Height: 4096, Tiles: 256})
    if err != nil {
        log.Fatal(err)
    }
    if created {
        log.Print("this node hosts the coordinator")
    }

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)
    <-sig
}
```

The coordinator then spawns workers with `Spawn`, which places them across the cluster.

## Mailboxes and delivery

Mailboxes are **unbounded**. `Send` never blocks and never applies backpressure. A slow consumer
accumulates messages until the node exhausts memory.

Each mailbox holds two queues: an outer queue appended to by senders under a mutex, and an inner
queue owned exclusively by the actor goroutine. The inner queue is refilled from the outer queue by
a single buffer swap when it drains, giving one lock acquisition per batch rather than per message.
Current depth is exported as a load metric and
published to the rest of the cluster; see [Membership and liveness](#membership-and-liveness).

Guarantees:

- **At-most-once delivery.** Messages are not retransmitted.
- **FIFO per sender-receiver pair**, within a connection epoch.
- **Copy semantics.** All messages are serialized, including local sends. Actors never share memory,
  and behaviour does not change with placement.

Messages are encoded with `encoding/gob` against a type-name registry. `Register` registers a
behavior's argument and message types; a type used only as a `Request` reply is registered with
`RegisterMessage`. Message types must be encodable: channels, functions, and unexported fields are
not transmissible.

## Membership and liveness

The node set is fixed by configuration, so membership is never used for discovery — every node
knows every peer's addresses before it starts. What configuration cannot state is which of those
peers are running at any given moment, and how loaded they are. SWIM supplies both: it is the
framework's failure detector, and the bus over which each node's statistics reach the others. It
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

**What each node publishes.** Alongside its liveness state, every node attaches a small record to
its gossip traffic and refreshes it every `meta_refresh`: its node id, its current epoch, its live
actor count, its aggregate mailbox depth, and the CPU utilisation of its own process over the last
interval. That record has to fit inside a bounded metadata allowance, so it is fixed-width integers
throughout and carries no strings. Because it rides on traffic the failure detector is sending
anyway, disseminating statistics costs no messages of its own.

Carrying the epoch here is what lets a restart be told from a recovery. A node that was unreachable
and returns with the epoch it had before was merely slow, and references to its actors are still
good; one that returns with a higher epoch is a new process, and every actor the cluster remembers
on it is gone.

**The local view.** Each node folds all of this into a table of node id to state, epoch and
statistics, read without a network call. It is consulted on every path that needs to know something
about another node:

- `Ref[T].Send` to a remote actor checks liveness first, and fails with `ErrNodeDown` before
  touching the transport.
- `Spawn` reads the alive set and the statistics to choose a host for a new actor.
- An epoch change invalidates cached references to the previous incarnation and drops that node's
  entries from the name registry.
- The name registry answers `ErrRegistryUnavailable` while a shard owner is not alive, and
  re-announces the names it hosts when that owner becomes alive again.

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
  and its names leave the registry until it re-announces them on returning. Nothing in the design
  elects a leader or needs a quorum, so a divergent view cannot corrupt state, only waste work.
- Statistics are stale by up to one refresh interval plus propagation time. Placement is built to
  tolerate that: it needs the comparison between two sampled nodes to be right more often than
  chance, not to be right every time.
- Membership never changes name ownership. `hash(name) % N` is taken over the configured node set
  rather than the alive one, so a dead node's shard stays its shard and no failure triggers
  rehashing.

`Shutdown` leaves the cluster gracefully, so peers record the departure immediately instead of
waiting out a suspicion timeout. A node killed outright is detected on the ordinary path above.

## Naming

Named actors are recorded in a distributed registry. The owner of a name is
`hash(name) % N`, over the statically configured node set. Since membership never changes, ownership
is fixed and no rehashing occurs.

The registry stores `name -> (ActorID, message type name)`. It determines the **registry authority
for a name, not where the actor runs**; placement is independent.

The registry is an index over state the cluster already holds: every node knows which named actors
it hosts. When a node restarts and reclaims its shard, it broadcasts a rebuild request and
reconstructs the shard from the other nodes' replies.

Availability:

- If a shard owner is down, names in that shard cannot be registered or resolved for the first time.
- Names already resolved continue to work; callers cache the `ActorID`, which stays valid until the
  actor terminates.
- Other shards are unaffected.

When a named actor terminates, the framework releases its name, and a subsequent `SpawnNamed` with
that name will construct a new actor. The release is delivered to the shard owner at most once: if
it is lost, the name stays bound to the dead actor — sends to it return `ErrActorDead` — until the
owner next rebuilds its shard. This is an accepted limitation.

## Placement

`Spawn` without options selects a target node by power-of-two-choices: two live nodes are sampled at
random and the less loaded is chosen. Sampling two and taking the better one, rather than scanning
every node for the true minimum, is what keeps placement from stampeding — against a view that is
slightly stale, an exhaustive search sends every concurrent spawn to the same node.

Load is a weighted sum of three signals gossiped by each node, each mapped onto `[0, 1)` before
weighting so that unlike quantities can be added:

- **Actor count** — the capacity term. It is the only one of the three that responds to a burst of
  freshly spawned actors, which carry no backlog and have not yet run.
- **Average mailbox depth**, the aggregate depth divided by the actor count — the congestion term.
  Being an average rather than a total, it does not penalise a node merely for hosting many actors.
- **Process CPU utilisation** over the last refresh interval — the saturation term. It measures the
  node's own process rather than its machine, which is what keeps the signal meaningful when
  several nodes share one host.

The weights and the half-saturation points of that mapping are the `placement` keys in the
configuration. A candidate that has not gossiped its statistics yet counts as maximally loaded, so
a node that has only just joined is never preferred. The score is approximate by construction,
being read from a view up to a refresh interval stale; see
[Membership and liveness](#membership-and-liveness).

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
| `ErrNodeDown` | Destination node is not alive in the local membership view, or `Spawn` after the local `Shutdown` | none, fails locally |
| `ErrNoRoute` | No transport stream to the destination node | none, fails locally |
| `ErrUnknownNode` | Destination `NodeID` is absent from the configuration | none, fails locally |
| `ErrNameNotFound` | Lookup of an unregistered or released name | one registry call |
| `ErrTypeMismatch` | `Lookup[T]` found a different message type, `Spawn` args or `T` do not match the behavior, or a `Request` type lacks `ReplyTo Ref[R]` | one registry call, or none |
| `ErrRegistryUnavailable` | The shard owner for this name is not reachable | one registry call |
| `ErrRegistryRebuilding` | The shard owner is reconstructing its shard; retry | one registry call |
| `ErrUnknownBehavior` | Target node has no behavior registered under that name | one round trip |
| `ErrUnknownType` | Message type is not registered with the codec | none, fails locally |
| `ErrNoLiveNodes` | Automatic placement found no alive node | none, fails locally |
| `ErrSystemNotStarted` | A deserialised `Ref[T]` was used before `goactor.Start` | none, fails locally |
| `ErrTimeout` | `Request` deadline expired | the timeout |

`ErrUnknownType` fails at local encode. The receiver-side check that answers with a `Nack` carrying
`ErrUnknownType` exists only as defence against mismatched binaries; such a `Nack` resolves a
pending `Request` or is dropped.

Consequences applications must handle:

- An actor that only sends and never expects replies receives no indication that its peer has died.
- A timeout does not distinguish a slow actor from a dead one. Retrying may duplicate work.

## Transport

gRPC over HTTP/2, one long-lived bidirectional stream per ordered node pair, with message batching.
Connections form a full mesh and are re-established automatically. Traffic is plaintext; transport
security is out of scope.

## Startup

1. Load the configuration and resolve the local node identity from `-id`.
2. Read and increment the epoch counter in the data directory.
3. Open transport streams to all configured peers.
4. Join the SWIM cluster, and begin publishing liveness and load statistics.
5. Run application code, which registers behaviors and spawns actors.

Nodes do not wait for the full cluster to become available before starting.

## Repository layout

```
goactor/
  actor.go            Actor[T], ActorContext[T], Ref[T]
  id.go               ActorID, NodeID re-exports, sequence allocator, epoch file
  system.go           node runtime, spawn, dispatch, panic recovery
  registry.go         behavior name -> constructor
  mailbox/            two-queue unbounded mailbox
  codec/              gob type registry, Ref[T] encoding, envelopes
  transport/          gRPC service, per-pair bidirectional streams
  cluster/            memberlist integration, epochs, load gossip
  names/              sharded name registry, shard rebuild
  placement/          power-of-two-choices selection
  config/             YAML parsing and validation
  internal/ident/     ActorID, NodeID definitions, shared by every subpackage
cmd/mandelbrot/       demo application
examples/             minimal programs showing library usage (pingpong_local: single node)
```

## Dependencies

- `github.com/hashicorp/memberlist` — SWIM membership and failure detection
- `google.golang.org/grpc` — inter-node transport
- `go.yaml.in/yaml/v3` — configuration
