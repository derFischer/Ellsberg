//go:build redis

package spec

import (
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

type State struct {
	Me              uint64
	State           StateType
	CurrentTerm     uint64
	VotedFor        uint64
	Votes           []bool
	Log             []mpb.Command
	CommitIndex     uint64
	MmatchIndex     []uint64
	Peers           []uint64
	Quorum          MajorityQuorum
	Connections     map[mpb.Ctx]Connection `json:"-"`
	PendingReconfig uint64
}
