package spec

import (
	"log"
	"math"
	"sort"

	"go.nyu.edu/protosim/raft/messagepb"
)

func Apply(s State, msg *messagepb.Message) State {
	s.CtxAlive(msg.From, msg.Ctx)
	switch msg.Type {
	case messagepb.MsgApp:
		s.AppendEntryRequestHandler(msg)
	case messagepb.MsgAppResp:
		s.AppendEntryReplyHandler(msg)
	case messagepb.MsgVote:
		s.RequestVoteRequestHandler(msg)
	case messagepb.MsgVoteResp:
		s.RequestVoteReplyHandler(msg)
	case messagepb.CheckLeader:
		s.CheckLeader()
	case messagepb.Timeout:
		s.BecomeCandidate()
	case messagepb.ClientNormal, messagepb.ClientKVPut, messagepb.ClientConfChange:
		s.ClientRequestHandler(msg)
	}
	if len(s.Log) == 0 {
		log.Fatalf("ERROR! length of the log becomes 0! message: %+v\n", msg)
	}
	return s
}

func (s *State) CtxAlive(id uint64, ctx messagepb.Ctx) {
	if ctx == nil {
		return
	}
	conn, exist := s.Connections[ctx]
	if exist && inSlice(conn.Ids, id) != -1 {
		return
	}
	new_map := make(map[messagepb.Ctx]Connection)
	for k, v := range s.Connections {
		new_map[k] = v
	}

	if !exist {
		new_map[ctx] = Connection{Ids: []uint64{id}}
		s.Connections = new_map
		return
	}

	new_ids := make([]uint64, len(conn.Ids))
	copy(new_ids, conn.Ids)
	new_ids = append(new_ids, id)
	new_map[ctx] = Connection{Ids: new_ids}
	s.Connections = new_map
	return
}

func (s *State) DropStaleResponse(MTerm uint64) bool {
	if MTerm < s.CurrentTerm {
		return true
	}
	return false
}

func (s *State) UpdateTerm(MTerm uint64) {
	if MTerm > s.CurrentTerm {
		s.CurrentTerm = MTerm
		s.Follower()
	}
}

func (s *State) Follower() {
	s.State = Follower
	s.VotedFor = NOONE
	s.Votes = make([]bool, len(s.Votes))
	s.Connections = make(map[messagepb.Ctx]Connection)
	s.MmatchIndex = make([]uint64, len(s.MmatchIndex))
}

func (s *State) AppendEntryReplyHandler(reply *messagepb.Message) {
	if s.DropStaleResponse(reply.Mterm) {
		return
	}
	s.UpdateTerm(reply.Mterm)
	// A response from a server that is not a member (e.g. a non-voting
	// server being added) does not count toward any quorum.
	if reply.Success && inSlice(s.Peers, reply.From) != -1 {
		s.UpdateMatchIndex(reply.From, max(reply.MatchIndex, s.MmatchIndex[inSlice(s.Peers, reply.From)]))
		s.UpdateCommitIndex()
	}
	return
}

func (s *State) UpdateMatchIndex(id uint64, index uint64) {
	m := make([]uint64, len(s.MmatchIndex))
	copy(m, s.MmatchIndex)
	s.MmatchIndex = m
	s.MmatchIndex[inSlice(s.Peers, id)] = index
}

func (s *State) RequestVoteRequestHandler(args *messagepb.Message) {
	if s.DropStaleResponse(args.Mterm) {
		return
	}
	s.UpdateTerm(args.Mterm)
	logOK := (args.IndexTerm > s.GetLastCommandTerm()) || (args.IndexTerm == s.GetLastCommandTerm() && args.Index >= s.GetLastCommandIndex())
	grant := (args.Mterm == s.CurrentTerm) && logOK && (s.VotedFor == NOONE || s.VotedFor == args.From)
	if grant {
		s.VotedFor = args.From
	}
	return
}

func (s *State) RequestVoteReplyHandler(reply *messagepb.Message) {
	if s.DropStaleResponse(reply.Mterm) {
		return
	}
	s.UpdateTerm(reply.Mterm)

	if reply.Success && s.State == Candidate {
		s.ReceiveVotes(reply.From)
	}
	return
}

func (s *State) ReceiveVotes(from uint64) {
	if inSlice(s.Peers, from) == -1 {
		// not a member: its vote does not count
		return
	}
	// copy on write
	v := make([]bool, len(s.Votes))
	copy(v, s.Votes)
	s.Votes = v
	s.Votes[inSlice(s.Peers, from)] = true
}

func (s *State) CheckLeader() {
	if s.State != Candidate {
		return
	}
	if s.Quorum.VoteSuccess(&s.Votes, &s.Peers) {
		s.State = Leader
		s.Votes = make([]bool, len(s.Votes))
		s.PendingReconfig = s.GetLastCommandIndex()
		// A new leader appends an empty entry for its term right away and
		// counts it toward its own match index, as etcd (becomeLeader) and
		// RedisRaft (raft_become_leader) do.
		s.Log = append(s.CopyLog(), messagepb.Command{
			CommandIndex: s.GetLastCommandIndex() + 1,
			CommandTerm:  s.CurrentTerm,
			Command:      messagepb.Op{Type: messagepb.EntryEmpty},
		})
		s.MmatchIndex = make([]uint64, len(s.MmatchIndex))
		s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
		s.Connections = make(map[messagepb.Ctx]Connection)
	}
}

func (s *State) BecomeCandidate() {
	s.State = Candidate
	s.CurrentTerm = s.CurrentTerm + 1
	s.VotedFor = s.Me
	s.Votes = make([]bool, len(s.Votes))
	s.Votes[inSlice(s.Peers, s.Me)] = true
	s.Connections = make(map[messagepb.Ctx]Connection)
}

func findMajority(array []uint64) uint64 {
	// cp := make([]uint64, len(array))
	// copy(cp, array)
	sort.Slice(array, func(i, j int) bool {
		return array[i] < array[j]
	})
	length := len(array)
	return array[(length-1)/2]
}

func (m *MajorityQuorum) CommitIndex(AckedIndex *[]uint64, Peers *[]uint64) uint64 {
	if len(*m) == 0 {
		return math.MaxUint64
	}
	result := []uint64{}
	for idx, peer := range *Peers {
		ok := (inSlice(*m, peer)) != -1
		if !ok {
			continue
		}
		result = append(result, (*AckedIndex)[idx])
	}

	for i := len(result); i < len(*m); i++ {
		result = append(result, 0)
	}

	return findMajority(result)
}

func (m *MajorityQuorum) VoteSuccess(AckedVote *[]bool, Peers *[]uint64) bool {
	if len(*m) == 0 {
		return true
	}
	votes := 0
	for idx, peer := range *Peers {
		ok := (inSlice(*m, peer)) != -1
		if !ok {
			continue
		}
		if (*AckedVote)[idx] {
			votes = votes + 1
		}
	}
	return votes >= (len(*m)/2 + 1)
}

func (j *JointQuorum) CommitIndex(AckedIndex *[]uint64, Peers *[]uint64) uint64 {
	idx1 := j[0].CommitIndex(AckedIndex, Peers)
	idx2 := j[1].CommitIndex(AckedIndex, Peers)
	if idx1 > idx2 {
		return idx2
	} else {
		return idx1
	}
}

func (j *JointQuorum) VoteSuccess(AckedVote *[]bool, Peers *[]uint64) bool {
	return j[0].VoteSuccess(AckedVote, Peers) && j[1].VoteSuccess(AckedVote, Peers)
}
