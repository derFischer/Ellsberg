//go:build redis

package spec

import (
	"math"
	"reflect"

	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func Matched(s State, end *EndState) (State, bool) {
	if end.End == nil {
		return s, true
	}
	switch end.End.(type) {
	case PartialState:
		return PartialStateMatched(&(end.End), s)
	case mpb.Message:
		if end.End.(mpb.Message).Type == mpb.ClientNormalResp && end.End.(mpb.Message).Success == false {
			return s, true
		}
		if end.End.(mpb.Message).Type == mpb.ClientNormalResp {
			current_idx := end.Req_index + 1
			if current_idx >= s.GetFirstCommandIndex() {
				current_idx = current_idx - s.GetFirstCommandIndex()
			} else {
				current_idx = 0
			}
			for i := current_idx; i < uint64(len(s.Log)); i++ {
				if reflect.DeepEqual(s.Log[i].Command.ReqID, end.End.(mpb.Message).ReqID) {
					return s, s.GetCommitIndex() >= i+s.GetFirstCommandIndex()
				}
			}

			if end.Req_index+1 < s.GetFirstCommandIndex() {
				return s, true
			} else {
				return s, false
			}
			// return s, true
		} else if end.End.(mpb.Message).Type == mpb.ClientROResp {
			// commit_term := s.GetCommand(s.GetCommitIndex()).CommandTerm
			valid := s.QuorumActive(end.Connections) && s.CommitIndex >= end.Req_index
			// fmt.Printf("valid: %+v\n", valid)
			return s, valid
		}
	}
	return s, true
}

func PartialStateMatched(end_s *interface{}, s State) (State, bool) {
	ps := (*end_s).(PartialState)
	// s := CopyState(st)
	if ps.Reply != NotReply {
		if ps.Reply == AppReply {
			if ps.ReplySuccess {
				return s, s.State == Follower && s.CommitIndex >= ps.CommitIndex && TermMatch(s, ps) && VotedForMatch(s, ps) && LogMatch(&s, ps.Log, ps.CommitIndex, true, false)
			} else {
				prev_log := make([]mpb.Command, 0)
				prev_log = append(prev_log, ps.Log[0])
				return s, StateMatch(s, ps) && TermMatch(s, ps) && VotedForMatch(s, ps) && (s.CurrentTerm > ps.ReqTerm || !LogMatch(&s, prev_log, ps.CommitIndex, true, false))
			}
		}
		if ps.Reply == VoteReply {
			logOK := ps.Log[0].CommandTerm > s.GetLastCommandTerm() || (ps.Log[0].CommandTerm == s.GetLastCommandTerm() && ps.Log[0].CommandIndex >= s.GetLastCommandIndex())
			if ps.ReplySuccess {
				return s, StateMatch(s, ps) && TermMatch(s, ps) && VotedForMatch(s, ps) && logOK
			} else {
				return s, StateMatch(s, ps) && TermMatch(s, ps) && (s.CurrentTerm > ps.ReqTerm || !logOK || (s.CurrentTerm == ps.ReqTerm && s.VotedFor != ps.VotedFor))
			}
		}
	}
	if ps.State == Candidate {
		return s, StateMatch(s, ps) && TermMatch(s, ps) && VotedForMatch(s, ps) && LogMatch(&s, ps.Log, math.MaxUint64, true, false) && CommitIndexMatch(s, ps)
	} else if ps.State == Leader {
		if ps.Log != nil && len(ps.Log) != 0 {
			last_index := ps.Log[len(ps.Log)-1].CommandIndex
			last_term := ps.Log[len(ps.Log)-1].CommandTerm
			// hack redis:
			if last_index <= INIT_NUM_PEERS*2 {
				bootstrapPadLog(&s, ps.Log, last_index, last_term)
				s.UpdateMatchIndex(s.Me, s.GetLastCommandIndex())
				s.CommitIndex = ps.CommitIndex
				return s, StateMatch(s, ps) && TermMatch(s, ps) // && CommitIndexMatch(s, ps)
			}
		}
		return s, StateMatch(s, ps) && TermMatch(s, ps) && LogMatch(&s, ps.Log, math.MaxUint64, true, true) && CommitIndexMatch(s, ps)
	} else if ps.State == Follower {
		return s, StateMatch(s, ps) && TermMatch(s, ps) && VotedForMatch(s, ps) && LogMatch(&s, ps.Log, math.MaxUint64, true, false) && CommitIndexMatch(s, ps)
	}
	return s, true
}

func LogMatch(s1 *State, pl []mpb.Command, commit_index uint64, matched bool, matched_last bool) bool {
	first_index1 := s1.GetFirstCommandIndex()
	last_index1 := s1.GetLastCommandIndex()
	if matched_last {
		if len(pl) == 0 {
			return true
		} else {
			if last_index1 > pl[len(pl)-1].CommandIndex {
				return false
			}
		}
	}
	for i := 0; i < len(pl); i++ {
		cmd := pl[i]
		command_index := cmd.CommandIndex
		if command_index == Unknown {
			continue
		}
		if last_index1 < command_index {
			if last_index1 == command_index-1 {
				if cmd.Command.Type == mpb.EntryEmpty && cmd.CommandTerm <= s1.CurrentTerm {
					s1.InsertEntries(cmd)
					return true
				}
			}
			if matched {
				return false
			} else {
				return true
			}
		}

		if first_index1 > command_index {
			continue
		}

		cmd2 := s1.Log[command_index-first_index1]
		if !commandMatch(cmd, cmd2) {
			if cmd.Command.Type == mpb.EntryEmpty && cmd.CommandTerm <= s1.CurrentTerm {
				s1.InsertEntries(cmd)
			}
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
