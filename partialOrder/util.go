package partialOrder

import (
	"math"
	"reflect"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/state"
	"go.etcd.io/etcd/v3/statepb"
)

func StateEqual(s1 state.State, s2 state.State) bool {
	if s1.CurrentTerm != s2.CurrentTerm || s1.State != s2.State || s1.VotedFor != s2.VotedFor {
		return false
	}
	if !reflect.DeepEqual(s1.Log, s2.Log) || s1.CommitIndex != s2.CommitIndex {
		return false
	}
	if s1.State == statepb.Candidate {
		return reflect.DeepEqual(s1.Votes, s2.Votes)
	}
	if s1.State == statepb.Leader {
		return reflect.DeepEqual(s1.MmatchIndex, s2.MmatchIndex)
	}
	return true
}

func CopyState(s state.State) state.State {
	votes := make([]bool, len(s.Votes))
	log := make([]statepb.Command, len(s.Log))
	mmatchIndex := make([]uint64, len(s.MmatchIndex))
	peers := make([]uint64, len(s.Peers))
	copy(votes, s.Votes)
	copy(log, s.Log)
	copy(mmatchIndex, s.MmatchIndex)
	copy(peers, s.Peers)
	r := state.State{Me: s.Me, State: s.State, CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor, Votes: votes, Log: log, CommitIndex: s.CommitIndex, MmatchIndex: mmatchIndex, Peers: peers}
	return r
}

func Apply(st state.State, msg messagepb.Message) (state.State, messagepb.Message) {
	s := CopyState(st)
	// defer fmt.Printf("after apply, the size of s log: %+v\n", len(s.Log))
	switch msg.Type {
	case messagepb.MsgApp:
		reply := s.AppendEntryRequestHandler(messagepb.AppendEntriesArgs{From: msg.From, To: msg.To, MTerm: msg.Mterm, MPrevLogIndex: msg.Index, MPrevLogTerm: msg.IndexTerm, MEntries: msg.MEntries, MCommitIndex: msg.CommitIndex})
		s.Truncate()
		return s, messagepb.Message{Type: messagepb.MsgAppResp, From: reply.From, To: reply.To, Mterm: reply.MTerm, Success: reply.MSuccess, MatchIndex: reply.MMatchIndex}
	case messagepb.MsgAppResp:
		s.AppendEntryReplyHandler(messagepb.AppendEntriesReply{From: msg.From, To: msg.To, MTerm: msg.Mterm, MSuccess: msg.Success, MMatchIndex: msg.MatchIndex})
		s.Truncate()
		return s, messagepb.Message{}
	case messagepb.MsgVote:
		reply := s.RequestVoteRequestHandler(messagepb.RequestVoteArgs{From: msg.From, To: msg.To, MTerm: msg.Mterm, MLastLogIndex: msg.Index, MLastLogTerm: msg.IndexTerm})
		s.Truncate()
		return s, messagepb.Message{Type: messagepb.MsgVoteResp, From: reply.From, To: reply.To, Mterm: reply.MTerm, Success: reply.MVoteGranted}
	case messagepb.MsgVoteResp:
		s.RequestVoteReplyHandler(messagepb.RequestVoteReply{From: msg.From, To: msg.To, MTerm: msg.Mterm, MVoteGranted: msg.Success})
		s.Truncate()
		return s, messagepb.Message{}
	case messagepb.ClientNormal, messagepb.ClientKVPut, messagepb.ClientConfChange:
		t := statepb.EntryUnknown
		if msg.Type == messagepb.ClientKVPut {
			t = statepb.EntryKVPut
		} else if msg.Type == messagepb.ClientNormal {
			t = statepb.EntryNormal
		} else if msg.Type == messagepb.ClientConfChange {
			t = statepb.EntryConfChange
		}
		s.ClientRequestHandler(statepb.Op{ReqID: msg.ReqID, Type: t, Key: msg.Key, Value: msg.Value, Data: msg.Data})
		s.Truncate()
		return s, messagepb.Message{}
	}
	return state.State{}, messagepb.Message{}
}

func MsgAppInference(msg messagepb.Message) state.PartialState {
	log := make([]statepb.Command, 0)
	log = append(log, statepb.Command{CommandIndex: msg.Index, CommandTerm: msg.IndexTerm, Command: statepb.Op{Type: statepb.EntryUnknown}})
	log = append(log, msg.MEntries...)
	return state.PartialState{State: statepb.Leader, CurrentTerm: msg.Mterm, VotedFor: statepb.Unknown, Log: log, CommitIndex: msg.CommitIndex}
}

func MsgVoteInference(msg messagepb.Message) state.PartialState {
	log := make([]statepb.Command, 0)
	log = append(log, statepb.Command{CommandIndex: msg.Index, CommandTerm: msg.IndexTerm})
	return state.PartialState{State: statepb.Candidate, CurrentTerm: msg.Mterm, VotedFor: msg.From, Log: log, CommitIndex: statepb.Unknown}
}

func Inference(msg messagepb.Message) (interface{}, messagepb.Message) {
	switch msg.Type {
	case messagepb.MsgApp:
		return MsgAppInference(msg), messagepb.Message{}
	case messagepb.MsgAppResp:
		return messagepb.Message{Type: messagepb.MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Mterm, Success: msg.Success, MatchIndex: msg.MatchIndex}, messagepb.Message{Type: messagepb.MsgApp, From: msg.To, To: msg.From, Mterm: msg.ReqTerm, Index: msg.Index, IndexTerm: msg.IndexTerm, MEntries: msg.MEntries, CommitIndex: msg.CommitIndex}
	case messagepb.MsgVote:
		return MsgVoteInference(msg), messagepb.Message{}
	case messagepb.MsgVoteResp:
		return messagepb.Message{Type: messagepb.MsgVoteResp, From: msg.From, To: msg.To, Mterm: msg.Mterm, Success: msg.Success}, messagepb.Message{Type: messagepb.MsgVote, From: msg.To, To: msg.From, Mterm: msg.ReqTerm, Index: msg.Index, IndexTerm: msg.IndexTerm}
	case messagepb.ClientNormalResp:
		return msg, messagepb.Message{}
	case messagepb.ClientROResp:
		return msg, messagepb.Message{}
	default:
		return nil, messagepb.Message{}
	}
}

func StateMatch(s1 state.State, s2 state.PartialState) bool {
	if s2.State == statepb.StateUnknown {
		return true
	}
	if s2.State == statepb.Leader {
		return s1.State == statepb.Leader
	}
	if s2.State == statepb.Follower {
		return s1.State == statepb.Follower
	} else {
		return s1.State == statepb.Candidate
	}
}

func TermMatch(s1 state.State, s2 state.PartialState) bool {
	if s2.CurrentTerm == statepb.Unknown {
		return true
	}
	return s1.CurrentTerm == s2.CurrentTerm
}

func VotedForMatch(s1 state.State, s2 state.PartialState) bool {
	if s2.VotedFor == statepb.Unknown {
		return true
	}
	return s1.VotedFor == s2.VotedFor
}

func VotesMatch(s1 state.State, s2 state.PartialState) bool {
	for i := 0; i < len(s2.Votes); i++ {
		if s2.Votes[i] == statepb.VotesUnknown {
			continue
		}
		if s2.Votes[i] == 1 {
			if s1.Votes[i] != true {
				return false
			}
		} else {
			if s1.Votes[i] != false {
				return false
			}
		}
	}
	return true
}

func commandMatch(cmd1 statepb.Command, cmd2 statepb.Command) bool {
	if cmd1.CommandIndex != cmd2.CommandIndex && cmd1.CommandIndex != statepb.Unknown {
		return false
	}
	if cmd1.CommandTerm != cmd2.CommandTerm && cmd1.CommandTerm != statepb.Unknown {
		return false
	}
	if cmd1.Command.Type == statepb.EntryUnknown {
		return true
	}
	if !reflect.DeepEqual(cmd1.Command.Data, cmd2.Command.Data) {
		return false
	}
	return true
}

func LogMatch(s1 state.State, pl []statepb.Command, commit_index uint64, matched bool) bool {
	// fmt.Printf("logmatch: log1: %+v\n\n  %+v\n\n", s1.Log, pl)
	first_index1 := s1.GetFirstCommandIndex()
	last_index1 := s1.GetLastCommandIndex()
	for i := 0; i < len(pl); i++ {
		cmd := pl[i]
		command_index := cmd.CommandIndex
		if last_index1 < command_index {
			if matched {
				return false
			} else {
				return true
			}
		}
		// if first_index1 < command_index {
		// 	continue
		// }
		if first_index1 > command_index {
			continue
		}

		cmd2 := s1.Log[command_index-first_index1]
		if !commandMatch(cmd, cmd2) {
			// fmt.Printf("cmd: %+v\n, cmd2: %+v\n\n\n", cmd, cmd2)
			if matched {
				return false
			}
			if cmd.CommandTerm == cmd2.CommandTerm || command_index <= commit_index {
				return false
			}
		}
	}
	return true
}

func CommitIndexMatch(s1 state.State, s2 state.PartialState) bool {
	if s2.CommitIndex == statepb.Unknown {
		return true
	}
	return s1.CommitIndex >= s2.CommitIndex
}

func ParitialStateEnabled(ps state.PartialState, from state.State, msg messagepb.Message) bool {
	if msg.Mterm > ps.CurrentTerm {
		return false
	}
	s, _ := Apply(from, msg)
	if !LogMatch(s, ps.Log, from.CommitIndex, false) {
		return false
	}
	if s.CurrentTerm == ps.CurrentTerm && s.VotedFor != ps.VotedFor && ps.VotedFor != statepb.Unknown {
		return false
	}
	if s.CurrentTerm == ps.CurrentTerm && !StateEnables(ps.State, s.State) {
		return false
	}
	if s.CommitIndex > ps.CommitIndex {
		return false
	}
	return true
}

func StateEnables(s1 statepb.StateType, s2 statepb.StateType) bool {
	if s1 == statepb.StateUnknown {
		return true
	} else if s1 == statepb.Leader {
		return s2 == statepb.Leader || s2 == statepb.Candidate
	} else {
		return s1 == s2
	}
}

func PartialStateMatched(ps state.PartialState, st state.State) (state.State, bool) {
	s := CopyState(st)
	if ps.State == statepb.Candidate {
		if ps.CurrentTerm > s.CurrentTerm {
			if LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps) {
				s.Candidate(ps.CurrentTerm)
				return s, true
			}
			return s, false
		} else if ps.CurrentTerm == s.CurrentTerm {
			return s, StateMatch(s, ps) && VotedForMatch(s, ps) && LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps)
		} else {
			return s, false
		}
	} else if ps.State == statepb.Leader {
		if s.State == statepb.Candidate {
			//hack:
			//fmt.Printf("MATCH CANDIDATE!!!!!!!!!!!!! STATE IS: %+v\n", s)
			if ps.Log != nil && len(ps.Log) != 0 {
				last_index := ps.Log[len(ps.Log)-1].CommandIndex
				if last_index <= 7 && TermMatch(s, ps) && CommitIndexMatch(s, ps) && s.TestLeader() {
					s.Leader()
					s.HackEntries(ps.Log)
					return s, true
				}
			}
			if TermMatch(s, ps) && LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps) && s.TestLeader() {
				s.Leader()
				return s, true
			}
			return s, false
		} else {
			//hack:
			if ps.Log != nil && len(ps.Log) != 0 {
				last_index := ps.Log[len(ps.Log)-1].CommandIndex
				if last_index <= 7 {
					s.HackEntries(ps.Log)
					return s, StateMatch(s, ps) && TermMatch(s, ps) && CommitIndexMatch(s, ps)
				}
			}
			// s.HackEntries(ps.Log)
			// fmt.Printf("ps is: %+v\n\n\n, state: %+v\n\n\n, match: %+v\n\n\n", ps, st, StateMatch(s, ps) && TermMatch(s, ps) && LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps))
			return s, StateMatch(s, ps) && TermMatch(s, ps) && LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps)
		}
	} else if ps.State == statepb.Follower {
		if s.State == statepb.Leader {
			s.State = statepb.Follower
			return s, TermMatch(s, ps) && VotedForMatch(s, ps) && LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps)
		}
		return s, StateMatch(s, ps) && TermMatch(s, ps) && VotedForMatch(s, ps) && LogMatch(s, ps.Log, math.MaxUint64, true) && CommitIndexMatch(s, ps)
	}
	return s, true
}
