package goactor

import (
	"fmt"
	"log/slog"
	"reflect"
	"sync"

	"github.com/tommasoprandin/goactor/codec"
)

type behaviorEntry struct {
	argType reflect.Type
	msgType reflect.Type
	spawn   func(s *System, args any, actorName string) (ActorID, error)
}

var behaviors = struct {
	sync.RWMutex
	m map[string]behaviorEntry
}{m: make(map[string]behaviorEntry)}

type Behavior[A, T any] struct {
	name string
}

// Register binds a behaviour name to its constructor and registers A and T with the wire codec.
// Assign the result to a package-level variable; registering a name twice panics. The constructor
// must not call SpawnNamed or Lookup: it runs under the lock of the table they use.
func Register[A, T any](name string, ctor func(args A) Actor[T]) Behavior[A, T] {
	codec.RegisterType[A]("") // take A's package name
	codec.RegisterType[T]("")
	argType := reflect.TypeFor[A]()
	e := behaviorEntry{
		argType: argType,
		msgType: reflect.TypeFor[T](),
		spawn: func(s *System, args any, actorName string) (id ActorID, err error) {
			a, ok := args.(A) // type assert conversion
			if !ok {
				return ActorID{}, fmt.Errorf("%w: behavior %q takes %v, got %T", ErrTypeMismatch, name, argType, args)
			}
			// On a remote spawn this is a gRPC handler, which would take the whole node down.
			defer func() {
				if r := recover(); r != nil {
					slog.Error("goactor: constructor panicked", "behavior", name, "panic", r)
					err = fmt.Errorf("%w: constructor of %q panicked: %v", ErrActorDead, name, r)
				}
			}()
			return spawnLocal(s, ctor(a), actorName)
		},
	}

	behaviors.Lock()
	defer behaviors.Unlock()
	if _, ok := behaviors.m[name]; ok {
		panic(fmt.Sprintf("goactor: behavior %q registered twice", name))
	}
	behaviors.m[name] = e
	return Behavior[A, T]{name: name}
}

// RegisterMessage registers T with the wire codec under name. It is needed only for types that no
// behaviour takes or receives, such as Request replies.
func RegisterMessage[T any](name string) { codec.RegisterType[T](name) }

func lookupBehavior(name string) (behaviorEntry, bool) {
	behaviors.RLock()
	defer behaviors.RUnlock()
	e, ok := behaviors.m[name]
	return e, ok
}

// checkBehavior guards against a Behavior converted to another instantiation, which compiles.
func checkBehavior[A, T any](b Behavior[A, T]) (behaviorEntry, error) {
	e, ok := lookupBehavior(b.name)
	if !ok {
		return e, fmt.Errorf("%w: %q", ErrUnknownBehavior, b.name)
	}
	if a := reflect.TypeFor[A](); e.argType != a {
		return e, fmt.Errorf("%w: behavior %q takes %v, not %v", ErrTypeMismatch, b.name, e.argType, a)
	}
	if t := reflect.TypeFor[T](); e.msgType != t {
		return e, fmt.Errorf("%w: behavior %q receives %v, not %v", ErrTypeMismatch, b.name, e.msgType, t)
	}
	return e, nil
}
