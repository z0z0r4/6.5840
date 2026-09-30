package kvsrv

import (
	"6.5840/kvsrv1/rpc"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}
