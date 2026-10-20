package goactor

import (
	"errors"
	"testing"

	"github.com/tommasoprandin/goactor/codec"
)

type embedsRef struct {
	Ref[int]
	N int
}

func init() {
	RegisterMessage[embedsRef]("test.embedsRef")
}

func roundTrip(t *testing.T, r Ref[int]) Ref[int] {
	t.Helper()
	name, b, err := codec.Marshal(query{N: 1, ReplyTo: r})
	if err != nil {
		t.Fatal(err)
	}
	v, err := codec.Unmarshal(name, b)
	if err != nil {
		t.Fatal(err)
	}
	return v.(query).ReplyTo
}

func TestRefEncoding(t *testing.T) {
	id := ActorID{Node: 1, Epoch: 2, Seq: 3}
	if r := roundTrip(t, Ref[int]{id: id}); r.ID() != id {
		t.Fatalf("decoded %v, want %v", r.ID(), id)
	} else if err := r.Send(1); !errors.Is(err, ErrSystemNotStarted) {
		t.Fatalf("send before Start: %v", err)
	}

	s := newSystem(t)
	got := make(chan int, 1)
	r := roundTrip(t, spawnProbe(t, s, hooks{receive: func(i int) { got <- i }}))
	if err := r.Send(5); err != nil {
		t.Fatal(err)
	}
	if v := <-got; v != 5 {
		t.Fatalf("got %d", v)
	}

	if _, _, err := codec.Marshal(embedsRef{N: 1}); err == nil {
		t.Fatal("struct embedding Ref encoded")
	}
}

func TestSingleSystem(t *testing.T) {
	newSystem(t)
	if _, err := Start(nil, 1); !errors.Is(err, errRunning) {
		t.Fatalf("second Start: %v", err)
	}
}
