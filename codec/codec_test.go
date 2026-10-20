package codec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/tommasoprandin/goactor/internal/ident"
)

type inner struct {
	Tags []string
	M    map[string]int
}

type msg struct {
	N     int
	In    inner
	Items []inner
	Ptr   *inner
}

type withChan struct {
	N    int
	Done chan int
}

type withFunc struct {
	In struct{ F func() }
}

type blob struct {
	Index int
	Data  []byte
}

type unregistered struct{ N int }

func init() {
	RegisterType[msg]("")
	RegisterType[withChan]("")
	RegisterType[withFunc]("")
	RegisterType[blob]("")
}

func newMsg() msg {
	return msg{
		N:     1,
		In:    inner{Tags: []string{"a", "b"}, M: map[string]int{"x": 1}},
		Items: []inner{{Tags: []string{"c"}}, {M: map[string]int{"y": 2}}},
		Ptr:   &inner{Tags: []string{"d"}},
	}
}

func TestRoundTrip(t *testing.T) {
	m := newMsg()
	name, b, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	m.In.Tags[0], m.In.M["x"], m.Items[1].M["y"], m.Ptr.Tags[0] = "z", 9, 9, "z"

	got, err := Unmarshal(name, b)
	if err != nil {
		t.Fatal(err)
	}
	if want := newMsg(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if _, err := Unmarshal(name, b[:len(b)-1]); err == nil {
		t.Fatal("truncated payload decoded")
	}
}

func TestUnknownType(t *testing.T) {
	if _, _, err := Marshal(unregistered{}); !errors.Is(err, ErrUnknownType) {
		t.Errorf("marshal: %v", err)
	}
	if _, err := Unmarshal("nope", nil); !errors.Is(err, ErrUnknownType) {
		t.Errorf("unmarshal: %v", err)
	}
}

func TestNotEncodable(t *testing.T) {
	for _, c := range []struct {
		v     any
		field string
	}{{withChan{}, "field Done"}, {withFunc{}, "field In.F"}} {
		if _, _, err := Marshal(c.v); err == nil || !strings.Contains(err.Error(), c.field) {
			t.Errorf("%T: %v", c.v, err)
		}
	}
}

func TestEnvelope(t *testing.T) {
	e := &Envelope{
		To:       ident.ActorID{Node: 1, Epoch: 2, Seq: 3},
		From:     ident.ActorID{Node: 4, Epoch: 5, Seq: 6},
		TypeName: "nope",
		Payload:  []byte{0xde, 0xad},
		Kind:     Nack,
	}
	b, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	if HeaderLen != 35 || len(b) != HeaderLen+len(e.TypeName)+len(e.Payload) {
		t.Fatalf("header %d, encoded %d bytes", HeaderLen, len(b))
	}
	got, err := Decode(b)
	if err != nil || !reflect.DeepEqual(got, e) {
		t.Fatalf("got %+v, %v", got, err)
	}
	if _, err := Unmarshal(got.TypeName, got.Payload); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("unmarshal: %v", err)
	}
	for i := range b {
		if _, err := Decode(b[:i]); err == nil {
			t.Fatalf("truncated to %d bytes: no error", i)
		}
	}
	if _, err := Decode(append(b, 0)); err == nil {
		t.Fatal("trailing byte: no error")
	}
}

func FuzzDecode(f *testing.F) {
	name, payload, err := Marshal(newMsg())
	if err != nil {
		f.Fatal(err)
	}
	b, _ := Encode(&Envelope{TypeName: name, Payload: payload})
	f.Add(b)
	f.Add(payload)
	f.Fuzz(func(t *testing.T, b []byte) {
		if e, err := Decode(b); err == nil {
			Unmarshal(e.TypeName, e.Payload)
		}
		Unmarshal(name, b)
	})
}

func BenchmarkCodec(b *testing.B) {
	for _, n := range []int{64, 4 << 10, 256 << 10} {
		m := blob{Index: 1, Data: make([]byte, n)}
		name, payload, err := Marshal(m)
		if err != nil {
			b.Fatal(err)
		}
		env, _ := Encode(&Envelope{TypeName: name, Payload: payload})
		b.Run(fmt.Sprintf("encode/%dB", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for b.Loop() {
				name, payload, _ := Marshal(m)
				Encode(&Envelope{TypeName: name, Payload: payload})
			}
		})
		b.Run(fmt.Sprintf("decode/%dB", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for b.Loop() {
				e, _ := Decode(env)
				Unmarshal(e.TypeName, e.Payload)
			}
		})
	}
}
