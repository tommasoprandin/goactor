package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/tommasoprandin/goactor/internal/ident"
)

type Kind uint8

const (
	User Kind = iota
	Nack
)

// Envelope is the unit of delivery, local or remote.
type Envelope struct {
	To ident.ActorID
	// For a Request, its ephemeral reply id, echoed by a Nack; zero otherwise.
	From     ident.ActorID
	TypeName string
	Payload  []byte
	Kind     Kind
}

// HeaderLen is the encoded size of an envelope without its type name and payload.
const HeaderLen = 1 + 2*ident.ActorIDLen + 2 + 4

var errMalformed = errors.New("goactor: malformed envelope")

// Encode lays e out big-endian as kind, to, from, len(TypeName), len(Payload), TypeName, Payload.
func Encode(e *Envelope) ([]byte, error) {
	if len(e.TypeName) > math.MaxUint16 || uint64(len(e.Payload)) > math.MaxUint32 {
		return nil, fmt.Errorf("goactor: envelope too large: %d byte type name, %d byte payload", len(e.TypeName), len(e.Payload))
	}
	b := make([]byte, 0, HeaderLen+len(e.TypeName)+len(e.Payload))
	b = append(b, byte(e.Kind))
	b, _ = e.To.AppendBinary(b)
	b, _ = e.From.AppendBinary(b)
	b = binary.BigEndian.AppendUint16(b, uint16(len(e.TypeName)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(e.Payload)))
	b = append(b, e.TypeName...)
	return append(b, e.Payload...), nil
}

// Decode reads the header of b and leaves the payload undecoded. Payload aliases b.
func Decode(b []byte) (*Envelope, error) {
	if len(b) < HeaderLen {
		return nil, fmt.Errorf("%w: %d bytes", errMalformed, len(b))
	}
	const id = ident.ActorIDLen
	e := &Envelope{Kind: Kind(b[0])}
	e.To.UnmarshalBinary(b[1 : 1+id])
	e.From.UnmarshalBinary(b[1+id : 1+2*id])
	n := uint64(binary.BigEndian.Uint16(b[HeaderLen-6:]))
	p := uint64(binary.BigEndian.Uint32(b[HeaderLen-4:]))
	if uint64(len(b)) != HeaderLen+n+p {
		return nil, fmt.Errorf("%w: %d bytes, header says %d", errMalformed, len(b), HeaderLen+n+p)
	}
	e.TypeName = string(b[HeaderLen : HeaderLen+n])
	e.Payload = b[HeaderLen+n:]
	return e, nil
}
