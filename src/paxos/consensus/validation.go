package consensus

import "fmt"

func (s *Node) validateCommand(op Command) error {
	if op.Noop {
		if op.ReqID != "" || op.Data != "" || op.ReadOnly {
			return fmt.Errorf("invalid no-op envelope")
		}
		return nil
	}
	if op.ReadOnly && op.ReqID != "" {
		return fmt.Errorf("read-only command cannot have a request ID")
	}
	return s.machine.Validate(op)
}
