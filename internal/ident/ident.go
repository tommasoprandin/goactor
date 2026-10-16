// Package ident defines the identity types shared by every goactor package. The root package
// re-exports them; it cannot define them itself because its subpackages need them.
package ident

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

type NodeID uint16

// ActorID is self-routing: Node locates the hosting process and Epoch its incarnation. The zero
// value identifies no actor.
type ActorID struct {
	Node  NodeID
	Epoch uint32
	Seq   uint64
}

// in bytes
const actorIDLen = 14

// String formats the id as node.epoch.seq.
func (a ActorID) String() string {
	return fmt.Sprintf("%d.%d.%d", a.Node, a.Epoch, a.Seq)
}

func (a ActorID) IsZero() bool {
	return a == ActorID{}
}

func ParseActorID(s string) (ActorID, error) {
	f := strings.Split(s, ".")
	if len(f) == 3 {
		n, err1 := strconv.ParseUint(f[0], 10, 16)
		e, err2 := strconv.ParseUint(f[1], 10, 32)
		q, err3 := strconv.ParseUint(f[2], 10, 64)
		if err1 == nil && err2 == nil && err3 == nil {
			return ActorID{Node: NodeID(n), Epoch: uint32(e), Seq: q}, nil
		}
	}
	return ActorID{}, fmt.Errorf("goactor: invalid actor id %q", s)
}

// MarshalBinary encodes the id as 14 big-endian bytes: node, epoch, seq.
func (a ActorID) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, actorIDLen)
	b = binary.BigEndian.AppendUint16(b, uint16(a.Node))
	b = binary.BigEndian.AppendUint32(b, a.Epoch)
	return binary.BigEndian.AppendUint64(b, a.Seq), nil
}

func (a *ActorID) UnmarshalBinary(b []byte) error {
	if len(b) != actorIDLen {
		return fmt.Errorf("goactor: actor id is %d bytes, want %d", len(b), actorIDLen)
	}
	a.Node = NodeID(binary.BigEndian.Uint16(b))
	a.Epoch = binary.BigEndian.Uint32(b[2:])
	a.Seq = binary.BigEndian.Uint64(b[6:])
	return nil
}
