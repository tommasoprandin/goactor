package goactor

import (
	"fmt"
	"reflect"
	"sync"
)

type behaviorEntry struct {
	argType reflect.Type
	msgType reflect.Type
	spawn   func(s *System, args any) (ActorID, error)
}

var behaviors = struct {
	sync.RWMutex
	m map[string]behaviorEntry
}{m: make(map[string]behaviorEntry)}

// Register binds a behaviour name to its constructor. Call it from init; registering a name twice
// panics.
func Register[A, T any](name string, ctor func(args A) Actor[T]) {
	argType := reflect.TypeFor[A]()
	e := behaviorEntry{
		argType: argType,
		msgType: reflect.TypeFor[T](),
		spawn: func(s *System, args any) (ActorID, error) {
			a, ok := args.(A)
			if !ok {
				return ActorID{}, fmt.Errorf("%w: behavior %q takes %v, got %T", ErrTypeMismatch, name, argType, args)
			}
			return spawnLocal(s, ctor(a))
		},
	}

	behaviors.Lock()
	defer behaviors.Unlock()
	if _, ok := behaviors.m[name]; ok {
		panic(fmt.Sprintf("goactor: behavior %q registered twice", name))
	}
	behaviors.m[name] = e
}

func lookupBehavior(name string) (behaviorEntry, bool) {
	behaviors.RLock()
	defer behaviors.RUnlock()
	e, ok := behaviors.m[name]
	return e, ok
}
