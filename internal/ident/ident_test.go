package ident

import (
	"encoding/hex"
	"math"
	"testing"
)

func TestActorIDRoundTrip(t *testing.T) {
	for _, id := range []ActorID{
		{},
		{Node: math.MaxUint16},
		{Epoch: math.MaxUint32},
		{Seq: math.MaxUint64},
		{Node: math.MaxUint16, Epoch: math.MaxUint32, Seq: math.MaxUint64},
	} {
		b, err := id.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var got ActorID
		if err := got.UnmarshalBinary(b); err != nil || got != id {
			t.Errorf("binary %v: got %v, %v", id, got, err)
		}
		if got, err := ParseActorID(id.String()); err != nil || got != id {
			t.Errorf("string %v: got %v, %v", id, got, err)
		}
	}
}

func TestActorIDWireLayout(t *testing.T) {
	b, _ := ActorID{Node: 1, Epoch: 2, Seq: 3}.MarshalBinary()
	if got, want := hex.EncodeToString(b), "0001"+"00000002"+"0000000000000003"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestActorIDRejectsMalformed(t *testing.T) {
	for _, s := range []string{"", "1.2", "1.2.3.4", "65536.1.1"} {
		if _, err := ParseActorID(s); err == nil {
			t.Errorf("%q: no error", s)
		}
	}
	var id ActorID
	if err := id.UnmarshalBinary(make([]byte, actorIDLen-1)); err == nil {
		t.Error("short binary: no error")
	}
}

func TestActorIDMapKey(t *testing.T) {
	m := map[ActorID]int{}
	m[ActorID{Node: 1, Epoch: 1, Seq: 1}] = 1
	m[ActorID{Node: 1, Epoch: 2, Seq: 1}] = 2
	if len(m) != 2 {
		t.Errorf("got %d keys, want 2", len(m))
	}
}
