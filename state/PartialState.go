package state

import (
	"go.etcd.io/etcd/v3/statepb"
)

type PartialState struct {
	State       statepb.StateType
	CurrentTerm uint64
	VotedFor    uint64
	Votes       []uint8
	Log         []statepb.Command
	CommitIndex uint64
}
