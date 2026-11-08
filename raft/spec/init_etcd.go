//go:build etcd

package spec

import mpb "go.nyu.edu/protosim/raft/messagepb"

// InitialState returns the state of server id when a cluster of peers starts:
// the log holds the initial configuration (an entry per peer) and is
// committed, and every server is a follower in term 1.
func InitialState(id uint64, peers []uint64) State {
	votes := make([]bool, len(peers))
	matchIndex := make([]uint64, len(peers))
	var q JointQuorum
	q[0] = make([]uint64, len(peers))
	copy(q[0], peers)
	q[1] = []uint64{}
	lg := make([]mpb.Command, len(peers)+1)
	for i := 0; i <= len(peers); i++ {
		lg[i] = mpb.Command{CommandIndex: uint64(i)}
	}
	lg[len(peers)].CommandTerm = 1
	return State{Me: id, State: Follower, CurrentTerm: 1, VotedFor: NOONE, Votes: votes, Log: lg, CommitIndex: lg[len(lg)-1].CommandIndex, MmatchIndex: matchIndex, Peers: peers, Quorum: q}
}
