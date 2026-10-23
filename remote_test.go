package goactor

import (
	"bytes"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tommasoprandin/goactor/codec"
)

type hop struct {
	N       int
	Body    []byte
	Trace   []int
	Route   []Ref[hop]
	ReplyTo Ref[hop]
}

// relay appends its tag to the trace and forwards the hop along its route, then to ReplyTo.
type relay struct{ tag int }

func (r *relay) Start(*ActorContext[hop]) {}

func (r *relay) Receive(_ *ActorContext[hop], h hop) {
	h.Trace = append(h.Trace, r.tag)
	next := h.ReplyTo
	if len(h.Route) > 0 {
		next, h.Route = h.Route[0], h.Route[1:]
	}
	next.Send(h)
}

func (r *relay) Stop(*ActorContext[hop], error) {}

type sink chan hop

func (sink) Start(*ActorContext[hop]) {}

func (s sink) Receive(_ *ActorContext[hop], h hop) { s <- h }

func (sink) Stop(*ActorContext[hop], error) {}

var (
	relayBehavior = Register("test.relay", func(tag int) Actor[hop] { return &relay{tag} })
	sinkBehavior  = Register("test.sink", func(ch chan hop) Actor[hop] { return sink(ch) })
)

func init() {
	scenarios["relay"] = func(s *System) ([]ActorID, error) {
		r, err := Spawn(s.Root(), relayBehavior, int(s.node))
		return []ActorID{r.ID()}, err
	}
}

func receive(t *testing.T, ch chan hop) hop {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(harnessTimeout):
		t.Fatal("nothing received")
		return hop{}
	}
}

func TestRemote(t *testing.T) {
	c := startCluster(t, 3, "relay")
	root := c.sys.Root()
	far := Ref[hop]{id: c.nodes[3].actors[0]}
	got := make(chan hop, 10_000)
	back, err := Spawn(root, sinkBehavior, got)
	if err != nil {
		t.Fatal(err)
	}

	body := bytes.Repeat([]byte("goactor"), 1000)
	if err := far.Send(hop{Body: body, ReplyTo: back}); err != nil {
		t.Fatal(err)
	}
	if h := receive(t, got); !bytes.Equal(h.Body, body) || !slices.Equal(h.Trace, []int{3}) {
		t.Fatalf("got %d bytes via %v", len(h.Body), h.Trace)
	}

	r, err := Request[hop, hop](root, far, hop{N: 7}, harnessTimeout)
	if err != nil || r.N != 7 || !slices.Equal(r.Trace, []int{3}) {
		t.Fatalf("request: %+v, %v", r, err)
	}

	const n = 10_000
	for i := range n {
		if err := far.Send(hop{N: i, ReplyTo: back}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		if h := receive(t, got); h.N != i {
			t.Fatalf("message %d arrived at position %d", h.N, i)
		}
	}
}

func TestRemoteStaleActor(t *testing.T) {
	c := startCluster(t, 3, "relay")
	stale := Ref[hop]{id: c.nodes[3].actors[0]}
	c.restart(3)
	fresh := Ref[hop]{id: c.nodes[3].actors[0]}
	if fresh.id.Seq != stale.id.Seq || fresh.id.Epoch == stale.id.Epoch {
		t.Fatalf("restart gave %v after %v", fresh.id, stale.id)
	}

	root := c.sys.Root()
	if _, err := Request[hop, hop](root, stale, hop{}, time.Minute); !errors.Is(err, ErrActorDead) {
		t.Fatalf("request to stale ref: %v", err)
	}
	if _, err := Request[hop, hop](root, fresh, hop{}, harnessTimeout); err != nil {
		t.Fatalf("request to fresh ref: %v", err)
	}
}

func TestLocationTransparency(t *testing.T) {
	c := startCluster(t, 3, "relay")
	root := c.sys.Root()
	local := make([]Ref[hop], 3)
	for i := range local {
		r, err := Spawn(root, relayBehavior, i+1)
		if err != nil {
			t.Fatal(err)
		}
		local[i] = r
	}
	split := []Ref[hop]{local[0], {id: c.nodes[2].actors[0]}, {id: c.nodes[3].actors[0]}}
	if a, b := traces(t, root, local), traces(t, root, split); !reflect.DeepEqual(a, b) {
		t.Fatalf("co-located %v, split %v", a, b)
	}
}

// traces sends requests along routes of varying length over relays and returns each reply's trace.
func traces(t *testing.T, ctx Context, relays []Ref[hop]) [][]int {
	t.Helper()
	var out [][]int
	for i := range 12 {
		var route []Ref[hop]
		for j := range i%4 + 1 {
			route = append(route, relays[(i+j)%len(relays)])
		}
		r, err := Request[hop, hop](ctx, route[0], hop{N: i, Route: route[1:]}, harnessTimeout)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r.Trace)
	}
	return out
}

type downView struct{}

func (downView) IsAlive(NodeID) bool { return false }

func (downView) Epoch(NodeID) (uint32, bool) { return 0, false }

func TestSendNodeDown(t *testing.T) {
	out := new(fakeOut)
	startSystem(t, downView{}, out)
	if err := (Ref[int]{id: ActorID{Node: 2, Epoch: 1, Seq: 1}}).Send(1); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("send: %v", err)
	}
	if n := len(out.envelopes()); n != 0 {
		t.Fatalf("%d transport calls", n)
	}
}

func TestUnknownTypeNack(t *testing.T) {
	out := new(fakeOut)
	s, in := startSystem(t, nil, out)
	got := make(chan int, 1)
	r := spawnProbe(t, s, hooks{receive: func(i int) { got <- i }})

	from := ActorID{Node: 2, Epoch: 1, Seq: 9}
	in.Deliver(&codec.Envelope{To: r.ID(), From: from, TypeName: "test.noSuchType"})
	sent := out.envelopes()
	if len(sent) != 1 || sent[0].Kind != codec.Nack || sent[0].To != from || len(sent[0].Payload) != 1 {
		t.Fatalf("sent %+v", sent)
	}
	if err := nackErrs[sent[0].Payload[0]]; err != ErrUnknownType {
		t.Fatalf("nack carries %v", err)
	}

	if err := r.Send(7); err != nil {
		t.Fatal(err)
	}
	if v := <-got; v != 7 {
		t.Fatalf("got %d", v)
	}
}
