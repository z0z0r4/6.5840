package consensus

import (
	"6.5840/paxos/statemachine"
)

type Command = statemachine.Command
type Status = statemachine.Status
type Result = statemachine.Result

const (
	Expired  = statemachine.Expired
	Redirect = statemachine.Redirect
	Applied  = statemachine.Applied
	Conflict = statemachine.Conflict
	Busy     = statemachine.Busy
	Rejected = statemachine.Rejected
)

type Config struct {
	ID              int
	Peers           []string
	DataDir         string
	Machine         statemachine.Machine
	SnapshotUpgrade func([]byte) (Snapshot, error)
}
type RequestRecord struct {
	Op     Command
	Seq    int
	Result string
}
