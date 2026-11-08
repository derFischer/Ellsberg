//go:build redis

package spec

import mpb "go.nyu.edu/protosim/raft/messagepb"

// InitialState returns the state of server id when a cluster of peers starts:
// server 1, which initializes the cluster, is the leader of term 1, and the
// others are followers.
func InitialState(id uint64, peers []uint64) State {
	votes := make([]bool, len(peers))
	matchIndex := make([]uint64, len(peers))
	q := make(MajorityQuorum, len(peers))
	copy(q, peers)
	lg := []mpb.Command{{CommandIndex: 0}}
	state := Follower
	if id == 1 {
		state = Leader
	}
	return State{Me: id, State: state, CurrentTerm: 1, VotedFor: NOONE, Votes: votes, Log: lg, CommitIndex: 3, MmatchIndex: matchIndex, Peers: peers, Quorum: q}
}
