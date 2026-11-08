package spec

import (
	"go.nyu.edu/protosim/raft/messagepb"
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

var INIT_NUM_PEERS uint64 = 0

type PartialState struct {
	State        StateType
	CurrentTerm  uint64
	VotedFor     uint64
	Log          []mpb.Command
	CommitIndex  uint64
	Reply        ReplyType
	ReplySuccess bool
	ReqTerm      uint64
	CommitInTerm bool
}

func (s *State) Inference(msg mpb.Message) interface{} {
	switch msg.Type {
	case mpb.MsgApp:
		return MsgAppInference(msg)
	case mpb.MsgAppResp:
		return MsgAppRespInference(msg)
	case mpb.MsgVote:
		return MsgVoteInference(msg)
	case mpb.MsgVoteResp:
		return MsgVoteRespInference(msg)
	case mpb.ClientNormalResp, mpb.ClientROResp:
		return msg
	default:
		return nil
	}
}

func MsgAppInference(msg mpb.Message) PartialState {
	log := make([]mpb.Command, 0)
	log = append(log, mpb.Command{CommandIndex: msg.Index, CommandTerm: msg.IndexTerm, Command: mpb.Op{Type: mpb.EntryUnknown}})
	log = append(log, msg.MEntries...)
	commitInTerm := (msg.Ctx != nil)
	return PartialState{State: Leader, CurrentTerm: msg.Mterm, VotedFor: Unknown, Log: log, CommitIndex: msg.CommitIndex, CommitInTerm: commitInTerm}
}

func MsgVoteInference(msg mpb.Message) PartialState {
	log := make([]mpb.Command, 0)
	log = append(log, mpb.Command{CommandIndex: msg.Index, CommandTerm: msg.IndexTerm, Command: mpb.Op{Type: mpb.EntryUnknown}})
	return PartialState{State: Candidate, CurrentTerm: msg.Mterm, VotedFor: msg.From, Log: log, CommitIndex: Unknown}
}

func MsgAppRespInference(msg mpb.Message) PartialState {
	log := make([]mpb.Command, 0)
	log = append(log, mpb.Command{CommandIndex: msg.Index, CommandTerm: msg.IndexTerm, Command: mpb.Op{Type: mpb.EntryUnknown}})
	log = append(log, msg.MEntries...)
	return PartialState{State: StateUnknown, CurrentTerm: msg.Mterm, VotedFor: Unknown, Log: log, CommitIndex: msg.CommitIndex, Reply: AppReply, ReplySuccess: msg.Success, ReqTerm: msg.ReqTerm}
}

func MsgVoteRespInference(msg mpb.Message) PartialState {
	log := make([]mpb.Command, 0)
	log = append(log, mpb.Command{CommandIndex: msg.Index, CommandTerm: msg.IndexTerm, Command: mpb.Op{Type: mpb.EntryUnknown}})
	return PartialState{State: StateUnknown, CurrentTerm: msg.Mterm, VotedFor: msg.To, Log: log, CommitIndex: Unknown, Reply: VoteReply, ReplySuccess: msg.Success, ReqTerm: msg.ReqTerm}
}

func (s *State) InsertEntries(entry mpb.Command) {
	l := make([]mpb.Command, len(s.Log)+1)
	start_index := s.GetFirstCommandIndex()
	index := entry.CommandIndex
	for i := 0; i < len(l); i++ {
		log_index := uint64(i) + start_index
		if log_index < index {
			l[i] = s.Log[i]
		} else if log_index == index {
			l[i] = entry
		} else {
			l[i] = s.Log[i-1]
			l[i].CommandIndex = l[i].CommandIndex + 1
		}
	}
	if entry.Command.Type == mpb.EntryConfChange {
		s.PendingReconfig = entry.CommandIndex
	}
	s.Log = l
}

func (s *State) QuorumActive(contexts []messagepb.Ctx) bool {
	connections := s.connectedPeers(contexts)
	return s.Quorum.MajorityAlive(connections)
}

func (s *State) GetCommitIndex() uint64 {
	return s.CommitIndex
}

func (m *MajorityQuorum) MajorityAlive(Connected_peers *[]uint64) bool {
	if len(*m) == 0 {
		return true
	}
	connected_members := 0
	for _, peer := range *Connected_peers {
		ok := (inSlice(*m, peer)) != -1
		if !ok {
			continue
		}
		connected_members = connected_members + 1
	}
	return connected_members >= (len(*m)/2 + 1)
}

func (j *JointQuorum) MajorityAlive(Connected_peers *[]uint64) bool {
	return j[0].MajorityAlive(Connected_peers) && j[1].MajorityAlive(Connected_peers)
}
