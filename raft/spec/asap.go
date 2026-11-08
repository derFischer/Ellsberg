package spec

import (
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func ApplyASAP(msg *mpb.Message, s *State) bool {
	if msg.Mterm < s.CurrentTerm {
		if msg.Ctx != nil {
			s.CtxAlive(msg.From, msg.Ctx)
		}
		return false
	}
	if msg.Type == mpb.MsgAppResp && msg.Mterm == s.CurrentTerm {
		if msg.Ctx != nil {
			s.CtxAlive(msg.From, msg.Ctx)
		}
		s.AppendEntryReplyHandler(msg)
		return true
	}
	if msg.Type == mpb.MsgVoteResp && (msg.Mterm == s.CurrentTerm && s.State == Leader) {
		if msg.Ctx != nil {
			s.CtxAlive(msg.From, msg.Ctx)
		}
		s.RequestVoteReplyHandler(msg)
		return true
	}
	if msg.Type == mpb.MsgApp && msg.Mterm == s.CurrentTerm && s.State == Follower && StaleApp(msg, s) {
		s.AppendEntryRequestHandler(msg)
		return true
	}
	if msg.Type == mpb.MsgVote && msg.Mterm == s.CurrentTerm && s.VotedFor != NOONE {
		return true
	}
	return false
}

// StaleApp reports whether the entries of an AppendEntries request are
// already in the log.
func StaleApp(msg *mpb.Message, state *State) bool {
	if len(msg.MEntries)+int(msg.Index) > int(state.GetLastCommandIndex()) {
		return false
	}
	if len(msg.MEntries) == 0 {
		local_entry := state.GetCommand(msg.Index)
		return local_entry.CommandTerm == msg.IndexTerm
	} else {
		local_entry := state.GetCommand(msg.Index + uint64(len(msg.MEntries)))
		last_entry := msg.MEntries[len(msg.MEntries)-1]
		return local_entry.CommandTerm == last_entry.CommandTerm
	}
}
