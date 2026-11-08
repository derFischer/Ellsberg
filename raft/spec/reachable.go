package spec

import (
	"reflect"

	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func Enabled(s State, msg *mpb.Message, end *EndState) (bool, bool, State) {
	// s = CopyState(s)
	switch end.End.(type) {
	case PartialState:
		return ParitialStateEnabled(&(end.End), s, msg)
	case mpb.Message:
		return true, true, s
	}
	return true, false, State{}
}

func ParitialStateEnabled(end_s *interface{}, from State, msg *mpb.Message) (bool, bool, State) {
	ps := (*end_s).(PartialState)
	if msg.Mterm > ps.CurrentTerm {
		return false, false, State{}
	}
	// Timeout only increases the term, and a match needs equal terms.
	if from.CurrentTerm >= ps.CurrentTerm && msg.Type == mpb.Timeout {
		return false, false, State{}
	}
	if msg.Type == mpb.ClientNormal || msg.Type == mpb.ClientKVPut || msg.Type == mpb.ClientConfChange {
		log_length := len(ps.Log)
		if log_length != 0 {
			if ps.Log[log_length-1].CommandIndex <= from.GetLastCommandIndex() {
				return false, false, State{}
			}
			ps_next_index := from.GetLastCommandIndex() + 1
			if ps_next_index > ps.Log[0].CommandIndex && !reflect.DeepEqual(ps.Log[ps_next_index-ps.Log[0].CommandIndex].Command.ReqID, msg.ReqID) {
				return false, false, State{}
			}
		}
	}
	return true, false, State{}
}
