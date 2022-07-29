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

func Apply(st state.State, msg messagepb.Msg) (state.State, interface{}) {
	t, m, _ := messagepb.MsgInterpreter(msg)
	s := CopyState(st)
	switch t {
	case messagepb.MsgApp:
		reply := s.AppendEntryRequestHandler(m.(messagepb.AppendEntriesArgs))
		return s, reply
	case messagepb.MsgAppResp:
		s.AppendEntryReplyHandler(m.(messagepb.AppendEntriesReply))
		return s, nil
	case messagepb.MsgVote:
		reply := s.RequestVoteRequestHandler(m.(messagepb.RequestVoteArgs))
		return s, reply
	case messagepb.MsgVoteResp:
		s.RequestVoteReplyHandler(m.(messagepb.RequestVoteReply))
		return s, nil
	case messagepb.ClientRequest:
		s.ClientRequestHandler(m.(statepb.Op))
		return s, nil
	}
	return state.State{}, nil
}

func MsgAppInference(msg messagepb.AppendEntriesArgs) state.PartialState {
	log := make([]statepb.Command, 0)
	log = append(log, statepb.Command{CommandIndex: msg.MPrevLogIndex, CommandTerm: msg.MPrevLogTerm, Command: statepb.Op{Type: statepb.EntryUnknown}})
	log = append(log, msg.MEntries...)
	// for i := 0; i < len(msg.MEntries); i++ {
	// 	en := msg.MEntries[i]
	// 	log = append(log, statepb.Command{CommandIndex: en.CommandIndex, CommandTerm: en.CommandTerm, Command: statepb.Op{ReqID: en.Command.ReqID, Type: en.Command.Type), Key: en.Command.Key, Value: en.Command.Value, Data: en.Command.Data}})
	// }
	return state.PartialState{State: statepb.Leader, CurrentTerm: msg.MTerm, VotedFor: statepb.Unknown, Log: log, CommitIndex: msg.MCommitIndex}
}

func MsgVoteInference(msg messagepb.RequestVoteArgs) state.PartialState {
	log := make([]statepb.Command, 0)
	log = append(log, statepb.Command{CommandIndex: msg.MLastLogIndex, CommandTerm: msg.MLastLogTerm})
	return state.PartialState{State: statepb.Candidate, CurrentTerm: msg.MTerm, VotedFor: msg.From, Log: log, CommitIndex: statepb.Unknown}
}

// func MsgVoteRespInference(msg messagepb.RequestVoteReply, req messagepb.RequestVoteArgs) PartialState {
// 	if msg.MVoteGranted {

// 	}
// }

func Inference(msg messagepb.Msg) (interface{}, interface{}) {
	t, m, req := messagepb.MsgInterpreter(msg)
	switch t {
	case messagepb.MsgApp:
		return MsgAppInference(m.(messagepb.AppendEntriesArgs)), nil
	case messagepb.MsgAppResp:
		return m, req
	case messagepb.MsgVote:
		return MsgVoteInference(m.(messagepb.RequestVoteArgs)), nil
	case messagepb.MsgVoteResp:
		return m, req
	case messagepb.ClientResp:
		return m, nil
	default:
		return nil, nil
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

func ParitialStateEnabled(ps state.PartialState, from state.State, msg messagepb.Msg) bool {
	s, _ := Apply(from, msg)
	// //fmt.Printf("!!!!!!!!!!!!!!!ENABLED FUNCTION, STATE: %+v\n, MESSAGE: %+v\n", ps, msg)
	if s.CurrentTerm > ps.CurrentTerm {
		return false
	}
	if !LogMatch(from, ps.Log, from.CommitIndex, false) {
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
