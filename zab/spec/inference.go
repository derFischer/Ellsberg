package spec

import (
	"go.nyu.edu/protosim/zab/messagepb"
)

type ElectionPartialState struct {
	// State statepb.StateType
	Vote         *Vote
	Round        int64
	CurrentEpoch int64
	// ConnectInfo      uint64
	ConnectingQuorum bool
	AckLDRecv        []uint64
}

type SyncedFollowerInfo struct {
	ID   uint64
	Type SyncType
	Zxid uint64
}

type PartialState struct {
	ElectionState  *ElectionPartialState
	State          StateType
	ZabState       ZabStateType
	AcceptEpoch    int64
	Log            []*Command
	LastCommit     uint64
	Learners       uint64
	Leader         uint64
	SyncedFollower *SyncedFollowerInfo
}

func Inference(msg *messagepb.Message) interface{} {
	if msg == nil {
		return nil
	}
	switch msg.Type {
	case messagepb.SYNC_DIFF:
		return MsgDiffInference(msg)
	case messagepb.SYNC_TRUNC:
		return MsgTruncInference(msg)
	case messagepb.FOLLOWERINFO:
		return MsgFollowerInfoInference(msg)
	case messagepb.LEADERINFO:
		return MsgLeaderInfoInference(msg)
	case messagepb.ACKEPOCH:
		return MsgACKEpochInference(msg)
	case messagepb.UPTODATE:
		return MsgUpToDateInference(msg)
	case messagepb.PROPOSAL:
		return MsgProposalInference(msg)
	case messagepb.ACK:
		return MsgAckInference(msg)
	case messagepb.ACKLD:
		return MsgACKLDInference(msg)
	case messagepb.COMMIT:
		return MsgCommitInference(msg)
	case messagepb.CLIENTACK, messagepb.CLIENTRORESP:
		return msg
	default:
		return nil
	}
}

func MsgDiffInference(msg *messagepb.Message) PartialState {
	commitZxid := Unknown
	if len(msg.Commits) > 0 {
		commitZxid = msg.Commits[len(msg.Commits)-1]
	}
	log := []*Command{}
	for _, e := range msg.Proposals {
		log = append(log, &Command{CommandIndex: e.Zxid, Command: Op{Type: EntryNormal, ReqID: ReqIndex{Session: e.ReqId.SessionId, Cxid: e.ReqId.Cxid}}})
	}
	end := PartialState{State: Leading, AcceptEpoch: int64(msg.NewleaderZxid >> 32), ZabState: SYNC | BROADCAST, SyncedFollower: &SyncedFollowerInfo{ID: msg.From, Type: DIFF, Zxid: msg.Zxid}, Log: log, LastCommit: commitZxid, Learners: msg.From}
	return end
}

func MsgTruncInference(msg *messagepb.Message) PartialState {
	commitZxid := msg.Zxid
	if len(msg.Commits) > 0 {
		commitZxid = msg.Commits[len(msg.Commits)-1]
	}
	log := []*Command{}
	for _, e := range msg.Proposals {
		log = append(log, &Command{CommandIndex: e.Zxid, Command: Op{Type: EntryNormal, ReqID: ReqIndex{Session: e.ReqId.SessionId, Cxid: e.ReqId.Cxid}}})
	}
	return PartialState{State: Leading, ZabState: SYNC | BROADCAST, SyncedFollower: &SyncedFollowerInfo{ID: msg.From, Type: TRUNC, Zxid: msg.Zxid}, Log: log, LastCommit: commitZxid, Learners: msg.From}
}

func MsgFollowerInfoInference(msg *messagepb.Message) PartialState {
	return PartialState{State: Following, ZabState: DISCOVERY, AcceptEpoch: int64(msg.Zxid >> 32)}
}

func MsgLeaderInfoInference(msg *messagepb.Message) PartialState {
	return PartialState{State: Leading, AcceptEpoch: int64(msg.Zxid >> 32), ElectionState: &ElectionPartialState{ConnectingQuorum: true}, Learners: msg.From}
}

func MsgACKEpochInference(msg *messagepb.Message) PartialState {
	if msg.Epoch == -1 {
		return PartialState{State: Following, ZabState: SYNC, Log: []*Command{&Command{CommandIndex: msg.Zxid, Command: Op{Type: EntryUnknown}}}, Leader: msg.From}
	} else {
		return PartialState{State: Following, ZabState: SYNC, ElectionState: &ElectionPartialState{CurrentEpoch: msg.Epoch}, Log: []*Command{&Command{CommandIndex: msg.Zxid, Command: Op{Type: EntryUnknown}}}, Leader: msg.From}
	}
}

// func MsgNewLeaderInference(msg *messagepb.Message) PartialState {
// 	return PartialState{State: Leading, ZabState: SYNC, AcceptEpoch: int64(msg.Zxid >> 32), Learners: msg.From}
// }

func MsgACKLDInference(msg *messagepb.Message) PartialState {
	return PartialState{State: Following, ZabState: SYNC, ElectionState: &ElectionPartialState{CurrentEpoch: int64(msg.Zxid >> 32)}, Leader: msg.From}
}

func MsgUpToDateInference(msg *messagepb.Message) PartialState {
	return PartialState{State: Leading, ZabState: BROADCAST, ElectionState: &ElectionPartialState{AckLDRecv: []uint64{msg.From}}, Learners: msg.From}
}

func MsgProposalInference(proposal *messagepb.Message) PartialState {
	return PartialState{State: Leading, ZabState: BROADCAST, Log: []*Command{&Command{CommandIndex: proposal.Zxid, Command: Op{Type: EntryNormal, ReqID: ReqIndex{Session: proposal.ReqId.SessionId, Cxid: proposal.ReqId.Cxid}}}}, LastCommit: Unknown, Learners: proposal.From}
}

func MsgAckInference(ack *messagepb.Message) PartialState {
	return PartialState{State: Following, ZabState: BROADCAST, Log: []*Command{&Command{CommandIndex: ack.Zxid, Command: Op{Type: EntryUnknown}}}, LastCommit: Unknown, Leader: ack.From}
}

func MsgCommitInference(commit *messagepb.Message) PartialState {
	return PartialState{State: Leading, ZabState: BROADCAST, LastCommit: commit.Zxid, Learners: commit.From}
}
