package spec

import (
	"reflect"

	"go.nyu.edu/protosim/zab/messagepb"
)

// EndState describes the states that can send a message (infer_inducing).
type EndState struct {
	End interface{}
}

var EPOCHUNKNOW int64 = 0

func Matched(s State, end *EndState) bool {
	switch end.End.(type) {
	case PartialState:
		return PartialStateMatched(&(end.End), &s)
	case messagepb.Message:
		if end.End.(messagepb.Message).Type == messagepb.CLIENTACK {
			return clientReplyMatched(&s, end.End.(messagepb.Message))
		}
		if end.End.(messagepb.Message).Type == messagepb.CLIENTRORESP {
			return true
		}
	}
	return true
}

func PartialStateMatched(end_s *interface{}, start *State) bool {
	ps := (*end_s).(PartialState)
	if !stateMatch(ps.State, start.ElectionStates.State) || !ZabStateMatch(ps.ZabState, start.ZabState) {
		return false
	}
	if ps.ElectionState != nil && !ElectionVarsMatch(ps.ElectionState, start) {
		return false
	}
	if ps.SyncedFollower != nil {
		//if sync mode is DIFF, then check whether the follower is in Election, and also whether the zxid recorded in Election is in the log
		if ps.SyncedFollower.Type == DIFF {
			ackepochinfo, ok := start.ElectionStates.Election[ps.SyncedFollower.ID]
			if !ok {
				return false
			}
			if ackepochinfo.LastProcessed > start.Log[len(start.Log)-1].CommandIndex {
				return false
			}
			if ackepochinfo.LastProcessed >= start.Log[0].CommandIndex {
				//scan the log check whether zxid exists in the log
				exist := false
				for i, e := range start.Log {
					if e.CommandIndex == ackepochinfo.LastProcessed {
						exist = true
						if len(ps.Log)+i+1 != len(start.Log) {
							return false
						}
						if !checkSyncLog(start, ps.Log, i+1, ps.LastCommit) {
							return false
						}
						break
					}
				}
				if !exist {
					return false
				}
			}
		}

		if ps.SyncedFollower.Type == TRUNC {
			ackepochinfo, ok := start.ElectionStates.Election[ps.SyncedFollower.ID]
			if !ok {
				return false
			}
			if ackepochinfo.LastProcessed <= ps.SyncedFollower.Zxid {
				return false
			}
			if ps.SyncedFollower.Zxid >= start.Log[0].CommandIndex {
				exist := false
				for i, e := range start.Log {
					if e.CommandIndex == ps.SyncedFollower.Zxid {
						exist = true
						if len(ps.Log)+i+1 != len(start.Log) {
							return false
						}
						if !checkSyncLog(start, ps.Log, i+1, ps.LastCommit) {
							return false
						}
						break
					}
				}
				if !exist {
					return false
				}
			}
		}
	}
	return (ps.AcceptEpoch == EPOCHUNKNOW || ps.AcceptEpoch == start.AcceptEpoch) && (ps.LastCommit == Unknown || ps.LastCommit <= start.LastCommitted) && LogMatch(ps.Log, start.Log) && (ps.Learners == Unknown || inSlice(*start.ElectionStates.Learners, ps.Learners) != -1) && (ps.Leader == Unknown || (start.ElectionStates.ConnectInfo != nil && ps.Leader == start.ElectionStates.ConnectInfo.Sid))
}

func ElectionVarsMatch(ps *ElectionPartialState, start *State) bool {
	if (ps.CurrentEpoch != EPOCHUNKNOW && ps.CurrentEpoch != start.Epoch) || (ps.ConnectingQuorum && !start.InQuorum(*start.ElectionStates.Connecting)) {
		return false
	}
	if ps.AckLDRecv != nil && len(ps.AckLDRecv) != 0 {
		for _, id := range ps.AckLDRecv {
			if inSlice(*start.ElectionStates.AckLdRecv, id) == -1 {
				return false
			}
		}
	}
	return true
}

func stateMatch(ps StateType, s StateType) bool {
	return ps == StateUnknown || ps == s
}

func ZabStateMatch(ps ZabStateType, s ZabStateType) bool {
	return ps == ZabStateUnknown || (ps&s) == s
}

// when call this, ps should only have 1 entry (the cases DIFF and TRUNC when the log has more entries are handled seperately) and it should be the last entry of the log of s
func LogMatch(ps []*Command, s []*Command) bool {
	return len(ps) == 0 || ((len(s) != 0) && CommandMatch(ps[len(ps)-1], s[len(s)-1]))
}

func CommandMatch(ps *Command, s *Command) bool {
	//fmt.Printf("command match: !!!!!!!!!!!!!!! ps: %+v, s: %+v\n", ps, s)
	return ps.CommandIndex < (1<<32|1) || ps.CommandIndex == s.CommandIndex && (ps.Command.Type == EntryUnknown || reflect.DeepEqual(ps.Command.ReqID, s.Command.ReqID))
}

func checkSyncLog(start *State, log []*Command, start_index int, commit uint64) bool {
	if commit != Unknown && start.LastCommitted != commit {
		return false
	}
	for i := start_index; i < len(start.Log); i++ {
		if start.Log[i].CommandIndex != log[i-start_index].CommandIndex {
			return false
		}
		if !reflect.DeepEqual(start.Log[i].Command.ReqID, log[i-start_index].Command.ReqID) {
			return false
		}
	}
	return true
}

// clientReplyMatched checks a reply to a client write: the request's entry must
// be in the log and committed. If the entry is not in the log, the reply is
// accepted only if the log has been trimmed (the log starts with the zxid-0
// placeholder until GC trims it, and GC only removes committed entries).
func clientReplyMatched(s *State, reply messagepb.Message) bool {
	ri := ReqIndex{Session: reply.ReqId.SessionId, Cxid: reply.ReqId.Cxid}
	for _, c := range s.Log {
		if reflect.DeepEqual(c.Command.ReqID, ri) {
			return s.LastCommitted >= c.CommandIndex
		}
	}
	return len(s.Log) > 0 && s.Log[0].CommandIndex != 0
}
