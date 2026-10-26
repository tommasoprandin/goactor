// Package codec serialises messages with gob against a registry of wire type names, and frames
// them in envelopes.
package codec

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var ErrUnknownType = errors.New("goactor: unknown type")

type entry struct {
	name string
	err  error
}

var types = struct {
	sync.RWMutex
	byName map[string]reflect.Type
	byType map[reflect.Type]entry
}{byName: make(map[string]reflect.Type), byType: make(map[reflect.Type]entry)}

// RegisterType binds T to a wire name, pkgpath.TypeName if name is empty. Call it from init;
// binding a name or a type to two different counterparts panics.
func RegisterType[T any](name string) {
	t := reflect.TypeFor[T]()
	if name == "" {
		name = defaultName(t)
	}
	types.Lock()
	defer types.Unlock()
	if e, ok := types.byType[t]; ok {
		if e.name != name {
			panic(fmt.Sprintf("goactor: type %v registered as both %q and %q", t, e.name, name))
		}
		return
	}
	if u, ok := types.byName[name]; ok {
		panic(fmt.Sprintf("goactor: type name %q bound to both %v and %v", name, u, t))
	}
	types.byName[name] = t
	types.byType[t] = entry{name: name, err: encodable(t)}
}

func defaultName(t reflect.Type) string {
	if t.PkgPath() == "" {
		return t.String()
	}
	return t.PkgPath() + "." + t.Name()
}

// TypeName returns the wire name t is registered under, empty if it is not registered.
func TypeName(t reflect.Type) string {
	types.RLock()
	defer types.RUnlock()
	return types.byType[t].name
}

// Marshal returns the wire name of v's dynamic type and v encoded with gob.
func Marshal(v any) (string, []byte, error) {
	t := reflect.TypeOf(v)
	types.RLock()
	e, ok := types.byType[t]
	types.RUnlock()
	if !ok {
		return "", nil, fmt.Errorf("%w: %v", ErrUnknownType, t)
	}
	if e.err != nil {
		return "", nil, e.err
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(v); err != nil {
		return "", nil, fmt.Errorf("goactor: encode %v: %w", t, err)
	}
	return e.name, b.Bytes(), nil
}

// Unmarshal decodes b into a new value of the type registered as name.
func Unmarshal(name string, b []byte) (v any, err error) {
	types.RLock()
	t, ok := types.byName[name]
	types.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, name)
	}
	// gob is not hardened against malformed input.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("goactor: decode %v: %v", t, r)
		}
	}()
	p := reflect.New(t)
	if err := gob.NewDecoder(bytes.NewReader(b)).DecodeValue(p); err != nil {
		return nil, fmt.Errorf("goactor: decode %v: %w", t, err)
	}
	return p.Elem().Interface(), nil
}

var customEncoders = []reflect.Type{
	reflect.TypeFor[gob.GobEncoder](),
	reflect.TypeFor[encoding.BinaryMarshaler](),
	reflect.TypeFor[encoding.TextMarshaler](),
}

func hasCustomEncoder(t reflect.Type) bool {
	for _, c := range customEncoders {
		if t.Implements(c) || reflect.PointerTo(t).Implements(c) {
			return true
		}
	}
	return false
}

// encodable rejects what gob would silently drop or truncate: chan and func fields, and embedded
// fields whose promoted encoder would stand in for the whole struct.
func encodable(t reflect.Type) error {
	if err := check(t, "", make(map[reflect.Type]bool)); err != nil {
		return fmt.Errorf("goactor: cannot encode %v: %w", t, err)
	}
	return nil
}

func check(t reflect.Type, path string, seen map[reflect.Type]bool) error {
	if seen[t] {
		return nil
	}
	seen[t] = true
	if t.Kind() == reflect.Struct {
		for i := range t.NumField() {
			if f := t.Field(i); f.Anonymous && hasCustomEncoder(f.Type) {
				return fmt.Errorf("field %s: embedded %v would encode the whole struct", join(path, f.Name), f.Type)
			}
		}
	}
	if hasCustomEncoder(t) {
		return nil
	}
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		if path == "" {
			return fmt.Errorf("%v is not encodable", t)
		}
		return fmt.Errorf("field %s: %v is not encodable", path, t)
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return check(t.Elem(), path, seen)
	case reflect.Map:
		if err := check(t.Key(), path, seen); err != nil {
			return err
		}
		return check(t.Elem(), path, seen)
	case reflect.Struct:
		for f := range t.Fields() {
			if f.IsExported() {
				if err := check(f.Type, join(path, f.Name), seen); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func join(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}
