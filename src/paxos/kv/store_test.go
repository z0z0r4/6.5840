package kv

import (
	"reflect"
	"testing"

	"6.5840/paxos/statemachine"
)

func TestStorageSnapshotAndInvalidRestore(t *testing.T) {
	var store Storage = New()
	store.Set("key", "before")
	image := store.Snapshot()
	store.Set("key", "after")
	if err := store.Restore(image); err != nil || store.Get("key") != "before" {
		t.Fatalf("snapshot restore: %v", err)
	}
	for _, bad := range []string{"null", "{", "[]", `{"key":12}`} {
		if err := store.Restore(bad); err == nil {
			t.Fatalf("accepted invalid snapshot %q", bad)
		}
		if store.Get("key") != "before" {
			t.Fatal("failed restore changed existing data")
		}
	}
}

type countingStorage struct {
	*Store
	writes int
}

func (s *countingStorage) Set(key, value string) { s.writes++; s.Store.Set(key, value) }
func TestStateMachineUsesIndependentStorage(t *testing.T) {
	backend := &countingStorage{Store: New()}
	machine := NewStateMachine(backend)
	write := Encode(Operation{ReqID: "write", Action: "set", Key: "k", Value: "v"})
	read := Encode(Operation{Action: "get", Key: "k"})
	if err := machine.Validate(write); err != nil {
		t.Fatal(err)
	}
	machine.Apply(write)
	if backend.writes != 1 || machine.Read(read) != "v" || machine.Apply(read) != "v" {
		t.Fatal("adapter did not use supplied storage")
	}
	before := backend.Values()
	for _, cmd := range []statemachine.Command{
		{Data: "not json"},
		{Data: `{"Action":"set","Key":"k","Value":"bad"}`, ReadOnly: true},
		{Data: `{"Action":"get","Key":"k"}`},
		{Data: `{"Action":"unknown"}`},
	} {
		if machine.Validate(cmd) == nil {
			t.Fatalf("accepted invalid command %+v", cmd)
		}
	}
	if !reflect.DeepEqual(before, backend.Values()) {
		t.Fatal("validation changed storage")
	}
}
