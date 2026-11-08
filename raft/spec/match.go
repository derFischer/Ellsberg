package spec

import (
	"reflect"

	mpb "go.nyu.edu/protosim/raft/messagepb"
)

// EndState describes the states that can send a message (infer_inducing).
type EndState struct {
	End         interface{}
	Connections []mpb.Ctx
	Req_index   uint64
}

func StateMatch(s1 State, s2 PartialState) bool {
	return s2.State == StateUnknown || (s1.State == s2.State)
}

func TermMatch(s1 State, s2 PartialState) bool {
	return s2.CurrentTerm == Unknown || s1.CurrentTerm == s2.CurrentTerm
}

func VotedForMatch(s1 State, s2 PartialState) bool {
	return s2.VotedFor == Unknown || s1.VotedFor == s2.VotedFor
}

func commandMatch(cmd1 mpb.Command, cmd2 mpb.Command) bool {
	if cmd1.CommandIndex != cmd2.CommandIndex && cmd1.CommandIndex != Unknown {
		return false
	}
	if cmd1.CommandTerm != cmd2.CommandTerm && cmd1.CommandTerm != Unknown {
		return false
	}
	if cmd1.Command.Type == mpb.EntryUnknown {
		return true
	}
	// Empty entries (e.g. a new leader's no-op) carry no request, so their
	// request ids are not compared (the interpreters report nil or 0).
	if cmd1.Command.Type == mpb.EntryEmpty && cmd2.Command.Type == mpb.EntryEmpty {
		return true
	}
	if cmd1.Command.Type == mpb.EntryConfChange {
		if !reflect.DeepEqual(cmd1.Command.ConfChange, cmd2.Command.ConfChange) {
			return false
		}
	} else if !reflect.DeepEqual(cmd1.Command.ReqID, cmd2.Command.ReqID) {
		return false
	}
	return true
}

func CommitIndexMatch(s1 State, s2 PartialState) bool {
	return s2.CommitIndex == Unknown || s1.CommitIndex >= s2.CommitIndex
}

// bootstrapPadLog extends the log up to lastIndex for a leader message inside
// the bootstrap prefix. Entries the message carries are copied (keeping their
// request ids, so replies to requests made during bootstrap can be checked);
// other indices get placeholder entries.
func bootstrapPadLog(s *State, msgLog []mpb.Command, lastIndex uint64, lastTerm uint64) {
	s.Log = s.CopyLog()
	for i := s.GetLastCommandIndex() + 1; i <= lastIndex; i++ {
		entry := mpb.Command{CommandTerm: lastTerm, CommandIndex: i}
		for _, c := range msgLog {
			if c.CommandIndex == i && c.Command.Type != mpb.EntryUnknown {
				entry = c
				break
			}
		}
		s.Log = append(s.Log, entry)
	}
}
