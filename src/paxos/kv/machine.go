package kv

import (
	"encoding/json"
	"fmt"

	"6.5840/paxos/statemachine"
)

type Operation struct{ ReqID, Action, Key, Value string }

// Encode constructs the opaque command accepted by a consensus node.
func Encode(op Operation) statemachine.Command {
	if op.Action == "nop" {
		return statemachine.Command{Noop: true}
	}
	data, _ := json.Marshal(struct{ Action, Key, Value string }{op.Action, op.Key, op.Value})
	return statemachine.Command{ReqID: op.ReqID, Data: string(data), ReadOnly: op.Action == "get"}
}

func Decode(cmd statemachine.Command) (Operation, error) {
	if cmd.Noop {
		return Operation{Action: "nop"}, nil
	}
	var op Operation
	if err := json.Unmarshal([]byte(cmd.Data), &op); err != nil {
		return op, err
	}
	op.ReqID = cmd.ReqID
	return op, nil
}

// StateMachine adapts a KV backend to the generic replicated-state interface.
type StateMachine struct{ storage Storage }

func NewStateMachine(storage Storage) *StateMachine { return &StateMachine{storage: storage} }
func (s *StateMachine) Kind() string                { return "kv/" + s.storage.Kind() }
func (*StateMachine) Validate(cmd statemachine.Command) error {
	op, err := Decode(cmd)
	if err != nil {
		return fmt.Errorf("invalid KV command: %w", err)
	}
	switch op.Action {
	case "set":
		if cmd.ReadOnly {
			return fmt.Errorf("write requires ReadOnly=false")
		}
	case "get":
		if !cmd.ReadOnly || cmd.ReqID != "" {
			return fmt.Errorf("read requires ReadOnly=true and no request ID")
		}
	default:
		return fmt.Errorf("unknown KV action %q", op.Action)
	}
	return nil
}

// Apply consumes a command already checked by Validate.
func (s *StateMachine) Apply(cmd statemachine.Command) string {
	op, _ := Decode(cmd)
	if op.Action == "set" {
		s.storage.Set(op.Key, op.Value)
		return ""
	}
	return s.storage.Get(op.Key)
}

func (s *StateMachine) Read(cmd statemachine.Command) string {
	op, _ := Decode(cmd)
	return s.storage.Get(op.Key)
}

func (s *StateMachine) Snapshot() string              { return s.storage.Snapshot() }
func (s *StateMachine) Restore(snapshot string) error { return s.storage.Restore(snapshot) }

var _ statemachine.Machine = (*StateMachine)(nil)
