package spec

import (
	"reflect"

	"go.nyu.edu/protosim/network"

	"go.nyu.edu/protosim/zab/messagepb"
)

// Pending is the pending messages of a server: one FIFO stream per connection.
type Pending = network.FIFO[messagepb.Message]

func Enabled(s *State, sets []Pending, msg *messagepb.Message, end *EndState) bool {
	switch end.End.(type) {
	case PartialState:
		return PartialStateEnabled(&(end.End), s, sets, msg)
	}
	return true
}

func PartialStateEnabled(end_s *interface{}, from *State, sets []Pending, msg *messagepb.Message) bool {
	ps := (*end_s).(PartialState)
	switch msg.Type {
	case messagepb.CLIENT:
		if len(ps.Log) != 0 {
			ps_last_command := ps.Log[len(ps.Log)-1]
			s_last_command := from.Log[len(from.Log)-1]
			if s_last_command.CommandIndex >= ps_last_command.CommandIndex {
				return false
			}
			for _, e := range ps.Log {
				if IsNextZxid(s_last_command.CommandIndex, e.CommandIndex) {
					return reflect.DeepEqual(e.Command.ReqID, ReqIndex{Session: msg.ReqId.SessionId, Cxid: msg.ReqId.Cxid})
				}
			}
		}
	case messagepb.ACK:
		if ps.LastCommit != Unknown {
			if msg.Zxid > ps.LastCommit {
				return false
			}
		}
	case messagepb.TIMEOUT, messagepb.TURNTOFOLLOWER, messagepb.TURNTOLEADER:
		if ps.State == Following && ps.ZabState == BROADCAST {
			// check whether an UPTODATE is pending
			for _, p := range sets {
				if contains(p, messagepb.UPTODATE) {
					return true
				}
			}
			return false
		}
		if ps.State == Leading && ps.ZabState == BROADCAST {
			// check whether an ACKLD is pending
			for _, p := range sets {
				if contains(p, messagepb.ACKLD) {
					return true
				}
			}
			return false
		}
		// case messagepb.TURNTOFOLLOWER, messagepb.TURNTOLEADER:
		// 	if ps.ZabState == BROADCAST && from.s.ZabState == BROADCAST {
		// 		return false, false, State{}
		// 	}
	}
	return true
}

// contains reports whether a message of type t is pending in p.
func contains(p Pending, t messagepb.MessageType) bool {
	found := false
	p.Each(func(m *messagepb.Message) bool {
		found = m.Type == t
		return !found
	})
	return found
}
