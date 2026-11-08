//go:build redis

package spec

import (
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func (s *State) AppendEntryRequestHandler(args *mpb.Message) {
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
		s.CommitIndex = max(s.CommitIndex, args.CommitIndex)
	}
	return
}

func (s *State) appendEntries(entries []mpb.Command) {
	l := make([]mpb.Command, len(s.Log))
	copy(l, s.Log)
	s.Log = l
	for _, command := range entries {
		current_index := s.GetCurrentIndex(command.CommandIndex)
		if current_index == TRUNCATED {
			continue
		}
		if current_index == NONEXIST {
			s.Log = append(s.Log, command)
			if command.Command.Type == mpb.EntryConfChange {
				s.ApplyReconfChange(command.Command.ConfChange)
			}
			continue
		}
		if s.Log[current_index].Command.Type == mpb.EntryConfChange {
			s.ApplyReconfChange([]mpb.ReconfigChange{rollbackEntry(s.Log[current_index].Command.ConfChange[0])})
		}
		s.Log[current_index] = command
		if command.Command.Type == mpb.EntryConfChange {
			s.ApplyReconfChange(command.Command.ConfChange)
		}
	}
}

func (s *State) ApplyReconfChange(changes []mpb.ReconfigChange) {
	if len(changes) != 1 {
		panic("only one change is allowed\n")
	}
	var q MajorityQuorum
	q = make([]uint64, len(s.Quorum))
	copy(q, s.Quorum)
	new_peers := make([]uint64, len(s.Peers))
	copy(new_peers, s.Peers)

	for _, change := range changes {
		if change.Type == mpb.AddNode {
			if inSlice(new_peers, change.Node) == -1 {
				new_peers = append(new_peers, change.Node)
			}
			if inSlice(q, change.Node) == -1 {
				q = append(q, change.Node)
			}
		}
		if change.Type == mpb.RemoveNode {
			rmSlice(&q, change.Node)
		}
	}
	s.Quorum = q
	s.setPeers(new_peers)
	return
}

func rollbackEntry(confchange mpb.ReconfigChange) mpb.ReconfigChange {
	var result mpb.ReconfigChange
	switch confchange.Type {
	case mpb.AddNode:
		result.Type = mpb.RemoveNode
	case mpb.RemoveNode:
		result.Type = mpb.AddNode
	}
	result.Node = confchange.Node
	return result
}

func (s *State) UpdateCommitIndex() {
	if s.State == Leader {
		majority := s.Quorum.CommitIndex(&s.MmatchIndex, &s.Peers)
		if majority > s.CommitIndex {
			command := s.GetCommand(majority)
			if command.CommandTerm == s.CurrentTerm {
				s.CommitIndex = max(command.CommandIndex, s.CommitIndex)
			}
		}
	}
}

func (s *State) ClientRequestHandler(args *mpb.Message) {
	var t mpb.EntryType
	if args.Type == mpb.ClientKVPut {
		t = mpb.EntryKVPut
	} else if args.Type == mpb.ClientNormal {
		t = mpb.EntryNormal
	} else if args.Type == mpb.ClientConfChange {
		t = mpb.EntryConfChange
	}

	if s.State == Leader {
		if t == mpb.EntryConfChange {
			alreadyPending := s.PendingReconfig > s.CommitIndex
			if alreadyPending {
				return
			}
			s.PendingReconfig = s.GetLastCommandIndex() + 1
			s.ApplyReconfChange(args.ConfChange)
		}
		l := make([]mpb.Command, len(s.Log))
		copy(l, s.Log)
		s.Log = l
		s.Log = append(s.Log, mpb.Command{CommandIndex: s.GetLastCommandIndex() + 1, CommandTerm: s.CurrentTerm, Command: mpb.Op{ReqID: args.ReqID, Type: t, ConfChange: args.ConfChange}})

		mi := make([]uint64, len(s.MmatchIndex))
		copy(mi, s.MmatchIndex)
		s.MmatchIndex = mi
		s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
	}
}
