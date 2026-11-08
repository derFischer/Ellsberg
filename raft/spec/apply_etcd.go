//go:build etcd

package spec

import (
	"log"

	"go.nyu.edu/protosim/raft/messagepb"
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func (s *State) AppendEntryRequestHandler(args *messagepb.Message) {
	if s.DropStaleResponse(args.Mterm) {
		return
	}
	s.UpdateTerm(args.Mterm)
	logOK := (args.Index == 0) || (args.Index > 0 && args.Index <= s.GetLastCommandIndex() && Equal(args.IndexTerm, s.GetCommandTerm(args.Index)))

	if args.Mterm == s.CurrentTerm && s.State == Follower && !logOK {
		return
	}
	if args.Mterm == s.CurrentTerm && s.State == Candidate {
		s.State = Follower
		s.Votes = make([]bool, len(s.Votes))
	}
	if args.Mterm == s.CurrentTerm && s.State == Follower && logOK {
		s.appendEntries(args.MEntries)
		old_commit := s.CommitIndex
		s.CommitIndex = max(s.CommitIndex, args.CommitIndex)
		for i := old_commit + 1; i <= s.CommitIndex; i++ {
			c := s.GetCommand(i)
			if c.Command.Type == mpb.EntryConfChange {
				//A follower will apply a conf change as soon as possible
				s.ApplyReconfChange(c.Command.ConfChange)
			}
		}
	}
	return
}

func (s *State) appendEntries(entries []mpb.Command) {
	s.Log = s.CopyLog()
	for _, command := range entries {
		current_index := s.GetCurrentIndex(command.CommandIndex)
		if current_index == TRUNCATED {
			continue
		}
		if current_index == NONEXIST {
			s.Log = append(s.Log, command)
			continue
		}
		s.Log[current_index] = command
	}
}

func (s *State) ApplyReconfChange(changes []mpb.ReconfigChange) {
	var q JointQuorum
	q[0] = make([]uint64, len(s.Quorum[0]))
	copy(q[0], s.Quorum[0])
	new_peers := make([]uint64, len(s.Peers))
	copy(new_peers, s.Peers)

	if len(changes) != 0 {
		if len(s.Quorum[1]) != 0 {
			log.Fatalf("Reconfig on a joint quorum, invalid!\n")
		}

		if len(changes) == 1 {
			//case 1: a single change, do not need to enter joint quorum:
			change := changes[0]
			if change.Type == mpb.AddNode {
				if inSlice(new_peers, change.Node) == -1 {
					new_peers = append(new_peers, change.Node)
				}
				if inSlice(q[0], change.Node) == -1 {
					q[0] = append(q[0], change.Node)
				}
			}
			if change.Type == mpb.RemoveNode {
				// rmSlice(&new_peers, change.Node)
				rmSlice(&q[0], change.Node)
			}
			q[1] = make([]uint64, 0)
		} else {
			//case 2: enter joint quorum
			q[1] = make([]uint64, len(q[0]))
			copy(q[1], q[0])
			for _, change := range changes {
				if change.Type == mpb.AddNode {
					if inSlice(s.Peers, change.Node) == -1 {
						new_peers = append(new_peers, change.Node)
					}
					if inSlice(q[0], change.Node) == -1 {
						q[0] = append(q[0], change.Node)
					}
				}
				if change.Type == mpb.RemoveNode {
					rmSlice(&(q[0]), change.Node)
				}
			}
		}
	} else {
		//leave joint
		if len(s.Quorum[1]) == 0 {
			panic("Trying to leave a single quorum!\n")
		}
		q[1] = make([]uint64, 0)
	}
	s.Quorum = q
	s.setPeers(new_peers)
	return
}

func (s *State) UpdateCommitIndex() {
	if s.State == Leader {
		majority := s.Quorum.CommitIndex(&s.MmatchIndex, &s.Peers)
		if majority > s.CommitIndex {
			old_commit := s.CommitIndex
			command := s.GetCommand(majority)
			if command.CommandTerm == s.CurrentTerm {
				s.CommitIndex = max(command.CommandIndex, s.CommitIndex)
				if s.PendingReconfig > old_commit && s.PendingReconfig <= s.CommitIndex {
					//if this is a single change or leave joint, apply it asap.
					c := s.GetCommand(s.PendingReconfig)
					if len(c.Command.ConfChange) == 0 || len(c.Command.ConfChange) == 1 && s.QuorumRelax(c.Command.ConfChange[0]) {
						s.LeaderApplyReconfig()
					}
				}
			}
		}
	}
}

func (s *State) LeaderApplyReconfig() {
	if s.PendingReconfig == 0 {
		return
	}
	entry := s.GetCommand(s.PendingReconfig)
	if entry.Command.Type != mpb.EntryConfChange {
		// The conservative marker set on becoming leader: nothing to apply,
		// and it stops blocking membership changes once committed.
		if s.PendingReconfig <= s.CommitIndex {
			s.PendingReconfig = 0
		}
		return
	}
	s.ApplyReconfChange(entry.Command.ConfChange)
	s.PendingReconfig = 0
}

// pendingConfChange reports whether a membership change is still pending.
// On becoming leader, PendingReconfig is set to the last log index (like etcd's
// pendingConfIndex); if that entry is not a membership change, it only blocks
// new membership changes until it is committed.
func (s *State) pendingConfChange() bool {
	if s.PendingReconfig == 0 {
		return false
	}
	if s.PendingReconfig > s.CommitIndex {
		return true
	}
	return s.GetCommand(s.PendingReconfig).Command.Type == mpb.EntryConfChange
}

func (s *State) QuorumRelax(confchange mpb.ReconfigChange) bool {
	if len(s.Quorum[0])%2 == 0 {
		return true
	} else {
		return false
	}
}

func (s *State) ClientRequestHandler(args *messagepb.Message) {
	var t mpb.EntryType
	if args.Type == messagepb.ClientKVPut {
		t = mpb.EntryKVPut
	} else if args.Type == messagepb.ClientNormal {
		t = mpb.EntryNormal
	} else if args.Type == messagepb.ClientConfChange {
		t = mpb.EntryConfChange
	}

	if s.State == Leader {
		if t == mpb.EntryConfChange {
			alreadyPending := s.pendingConfChange()
			alreadyJoint := len(s.Quorum[1]) != 0
			wantsLeaveJoint := len(args.ConfChange) == 0
			if alreadyPending || (alreadyJoint && !wantsLeaveJoint) || (!alreadyJoint && wantsLeaveJoint) {
				return
			}
			s.PendingReconfig = s.GetLastCommandIndex() + 1
		}
		s.Log = s.CopyLog()
		s.Log = append(s.Log, mpb.Command{CommandIndex: s.GetLastCommandIndex() + 1, CommandTerm: s.CurrentTerm, Command: mpb.Op{ReqID: args.ReqID, Type: t, ConfChange: args.ConfChange}})
		s.UpdateMatchIndex(s.Me, s.GetLastCommandIndex())
	}
}
