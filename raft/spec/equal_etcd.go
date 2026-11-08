//go:build etcd

package spec

import (
	"reflect"

	"go.nyu.edu/protosim/raft/messagepb"
)

func (s *State) Equal(s1 *State, contexts []messagepb.Ctx) bool {
	if s.Me != s1.Me || s.State != s1.State || s.CurrentTerm != s1.CurrentTerm || s.VotedFor != s1.VotedFor || s.CommitIndex != s1.CommitIndex || s.PendingReconfig != s1.PendingReconfig || !JointQuorumEqual(s.Quorum, s1.Quorum) {
		return false
	}
	if &(s.Log[0]) != &(s1.Log[0]) {
		if len(s.Log) != len(s1.Log) {
			return false
		}
		for i, v := range s.Log {
			if v.CommandIndex != s1.Log[i].CommandIndex || v.CommandTerm != s1.Log[i].CommandTerm || !reflect.DeepEqual(v.Command.ReqID, s1.Log[i].Command.ReqID) {
				return false
			}
		}
	}

	if &(s.Votes[0]) != &(s1.Votes[0]) {
		if !reflect.DeepEqual(s.Votes, s1.Votes) {
			return false
		}
	}
	if &(s.MmatchIndex[0]) != &(s1.MmatchIndex[0]) {
		if !reflect.DeepEqual(s.MmatchIndex, s1.MmatchIndex) {
			return false
		}
	}
	if &(s.Peers[0]) != &(s1.Peers[0]) {
		if !reflect.DeepEqual(s.Peers, s1.Peers) {
			return false
		}
	}

	// check context:
	if reflect.ValueOf(s.Connections).UnsafePointer() != reflect.ValueOf(s1.Connections).UnsafePointer() {
		return reflect.DeepEqual(s.Connections, s1.Connections)
	}
	return true
}
