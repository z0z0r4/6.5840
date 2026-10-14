// Package statemachine defines the boundary between consensus and application state.
package statemachine

// Command is an immutable, comparable envelope. Data belongs to the application.
// ReqID identifies writes for deduplication. ReadOnly permits lease reads.
// Noop is reserved for consensus recovery and has no application side effects.
type Command struct {
	ReqID    string
	Data     string
	ReadOnly bool
	Noop     bool
}

type Status uint8

const (
	Expired Status = iota
	Redirect
	Applied
	Conflict
	Busy
	Rejected
)

type Result struct {
	Status Status
	Leader int
	Value  string
	Error  string
}

// Machine is exclusively accessed under the consensus node's lock. Apply must
// be deterministic. Snapshot returns an immutable image; Restore must validate
// before replacing state. Read and Validate must not mutate state.
type Machine interface {
	Kind() string
	Validate(Command) error
	Apply(Command) string
	Read(Command) string
	Snapshot() string
	Restore(string) error
}
