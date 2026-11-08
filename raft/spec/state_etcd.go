//go:build etcd

package spec

import mpb "go.nyu.edu/protosim/raft/messagepb"

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
	Quorum          JointQuorum
	Connections     map[mpb.Ctx]Connection
	PendingReconfig uint64
}
