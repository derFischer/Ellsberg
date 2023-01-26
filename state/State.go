package state

import (
	"bytes"
	"log"
	"math"
	"reflect"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/simquorum"
	"go.etcd.io/etcd/v3/statepb"
)

const (
	TRUNCATED = math.MaxUint64
	NONEXIST  = math.MaxUint64 - 1
)

const NOONE = math.MaxUint64 - 1

type State struct {
	Me              uint64
	State           statepb.StateType
	CurrentTerm     uint64
	VotedFor        uint64
	Votes           []bool
	Log             []statepb.Command
	CommitIndex     uint64
	MmatchIndex     []uint64
	Peers           []uint64
	Quorum          simquorum.JointQuorum
	PendingReconfig uint64
}

func (s *State) ClientRequestHandler(args messagepb.Message) {
	t := statepb.EntryUnknown
	if args.Type == messagepb.ClientKVPut {
		t = statepb.EntryKVPut
	} else if args.Type == messagepb.ClientNormal {
		t = statepb.EntryNormal
	} else if args.Type == messagepb.ClientConfChange {
		t = statepb.EntryConfChange
	}

	if s.State == statepb.Leader {
		if t == statepb.EntryConfChange {
			alreadyPending := s.PendingReconfig > s.CommitIndex
			alreadyJoint := len(s.Quorum[1]) != 0
			wantsLeaveJoint := len(args.ConfChange) == 0
			if alreadyPending || (alreadyJoint && !wantsLeaveJoint) || (!alreadyJoint && wantsLeaveJoint) {
				return
			}
			s.PendingReconfig = s.GetLastCommandIndex() + 1
		}
		l := make([]statepb.Command, len(s.Log))
		copy(l, s.Log)
		s.Log = l
		s.Log = append(s.Log, statepb.Command{CommandIndex: s.GetLastCommandIndex() + 1, CommandTerm: s.CurrentTerm, Command: statepb.Op{ReqID: args.ReqID, Type: t, Key: args.Key, Value: args.Value, Data: args.Data, ConfChange: args.ConfChange}})

		// mi := make([]uint64, len(s.MmatchIndex))
		// copy(mi, s.MmatchIndex)
		// s.MmatchIndex = mi
		// s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
	}
}

func (s *State) RequestVoteRequestHandler(args messagepb.Message) messagepb.Message {
	r := messagepb.Message{Type: messagepb.MsgVoteResp, From: s.Me, To: args.From}
	if s.DropStaleResponse(args.Mterm) {
		r.Mterm = s.CurrentTerm
		r.Success = false
		return r
	}
	s.UpdateTerm(args.Mterm)
	logOK := (args.Index > s.GetLastCommandIndex()) || (args.IndexTerm == s.GetLastCommandTerm() && args.Index >= s.GetLastCommandIndex())
	grant := (args.Mterm == s.CurrentTerm) && logOK && (s.VotedFor == NOONE || s.VotedFor == args.From)
	if grant {
		s.VotedFor = args.From
	}
	r.Mterm = s.CurrentTerm
	r.Success = grant
	return r
}

func (s *State) AppendEntryRequestHandler(args messagepb.Message) messagepb.Message {
	r := messagepb.Message{Type: messagepb.MsgAppResp, From: s.Me, To: args.From}
	if s.DropStaleResponse(args.Mterm) {
		r.Mterm = s.CurrentTerm
		r.Success = false
		return r
	}
	s.UpdateTerm(args.Mterm)
	logOK := (args.Index == 0) || (args.Index > 0 && args.Index <= s.GetLastCommandIndex() && Equal(args.IndexTerm, s.GetCommandTerm(args.Index)))

	if args.Mterm == s.CurrentTerm && s.State == statepb.Follower && !logOK {
		r.Mterm = s.CurrentTerm
		r.Success = false
		return r
	}
	if args.Mterm == s.CurrentTerm && s.State == statepb.Candidate {
		s.State = statepb.Follower
	}
	if args.Mterm == s.CurrentTerm && s.State == statepb.Follower && logOK {
		s.appendEntries(args.MEntries)
		old_commit := s.CommitIndex
		s.CommitIndex = max(s.CommitIndex, args.CommitIndex)
		for i := old_commit; i <= s.CommitIndex; i++ {
			c := s.GetCommand(i)
			if c.Command.Type == statepb.EntryConfChange {
				//A follower will apply a conf change as soon as possible
				s.ApplyReconfChange(c.Command.ConfChange)
			}
		}
	}
	r.Mterm = s.CurrentTerm
	r.Success = (args.Mterm == s.CurrentTerm) && (s.State == statepb.Follower) && logOK
	if r.Success {
		r.MatchIndex = args.Index + uint64(len(args.MEntries))
	}
	return r
}

func (s *State) RequestVoteReplyHandler(reply messagepb.Message) {
	if s.DropStaleResponse(reply.Mterm) {
		return
	}
	s.UpdateTerm(reply.Mterm)

	if reply.Success {
		v := make([]bool, len(s.Votes))
		copy(v, s.Votes)
		s.Votes = v
		s.ReceiveVotes(reply.From)
	}
	return
}

func (s *State) AppendEntryReplyHandler(reply messagepb.Message) {
	if s.DropStaleResponse(reply.Mterm) {
		return
	}
	s.UpdateTerm(reply.Mterm)
	if reply.Success {
		m := make([]uint64, len(s.MmatchIndex))
		copy(m, s.MmatchIndex)
		s.MmatchIndex = m
		s.MmatchIndex[inSlice(s.Peers, reply.From)] = max(reply.MatchIndex, s.MmatchIndex[inSlice(s.Peers, reply.From)])
		// s.UpdateCommitIndex()
	}
	return
}

func (s *State) ReadIndexReplyHandler(reply messagepb.Message) {
	if s.DropStaleResponse(reply.Mterm) {
		return
	}
	s.UpdateTerm(reply.Mterm)
}

func (s *State) ApplyReconfChange(changes []statepb.ReconfigChange) {
	var q simquorum.JointQuorum
	q[0] = make([]uint64, len(s.Quorum[0]))
	copy(q[0], s.Quorum[0])
	new_peers := make([]uint64, len(s.Peers))
	copy(new_peers, s.Peers)

	if len(changes) != 0 {
		if len(s.Quorum[1]) != 0 {
			log.Fatalf("Reconfig on a joint quorum, invalid!\n")
		}

		if len(changes) == 1 {
			//case 1: a single change, do not need to enter joint quorum:
			change := changes[0]
			if change.Type == statepb.AddNode {
				if inSlice(new_peers, change.Node) != -1 {
					new_peers = append(new_peers, change.Node)
				}
				if inSlice(q[0], change.Node) != -1 {
					q[0] = append(q[0], change.Node)
				}
			}
			if change.Type == statepb.RemoveNode {
				// rmSlice(&new_peers, change.Node)
				rmSlice(&q[0], change.Node)
			}
			q[1] = make([]uint64, 0)
		} else {
			//case 2: enter joint quorum
			q[1] = make([]uint64, len(q[0]))
			copy(q[1], q[0])
			for _, change := range changes {
				if change.Type == statepb.AddNode {
					if inSlice(s.Peers, change.Node) != -1 {
						new_peers = append(new_peers, change.Node)
					}
					if inSlice(q[0], change.Node) != -1 {
						q[0] = append(q[0], change.Node)
					}
				}
				if change.Type == statepb.RemoveNode {
					rmSlice(&(q[0]), change.Node)
				}
			}
		}
	} else {
		//leave joint
		if len(s.Quorum[1]) == 0 {
			log.Fatalf("Trying to leave a single quorum!\n")
		}
		q[1] = make([]uint64, 0)
	}
	s.Quorum = q
	return
}

// other functions
func (s *State) Truncate() {
	s.LogGC()
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

func (s *State) TestLeader() bool {
	if s.State != statepb.Candidate {
		return false
	}
	return s.Quorum.VoteSuccess(&s.Votes, &s.Peers)
	// votes := 0
	// var i int
	// for i = 0; i < len(s.Peers); i++ {
	// 	if s.Votes[i] == true {
	// 		votes += 1
	// 	}
	// }
	// if votes > len(s.Peers)/2 {
	// 	return true
	// }
	// return false
}

func (s *State) CheckLeader() {
	if s.TestLeader() {
		s.Leader()
	}
}

func (s *State) Candidate(term uint64) {
	s.State = statepb.Candidate
	s.CurrentTerm = term
	s.VotedFor = s.Me
	v := make([]bool, len(s.Votes))
	copy(v, s.Votes)
	s.Votes = v
	for i := 0; i < len(s.Peers); i++ {
		s.Votes[i] = false
	}
	s.ReceiveVotes(s.Me)
}

func (s *State) BecomeCandidate() {
	s.Candidate(s.CurrentTerm + 1)
}

func (s *State) Follower() {
	s.State = statepb.Follower
	s.VotedFor = NOONE
}

func (s *State) Leader() {
	var i int
	s.State = statepb.Leader
	m := make([]uint64, len(s.MmatchIndex))
	copy(m, s.MmatchIndex)
	s.MmatchIndex = m

	for i = 0; i < len(s.Peers); i++ {
		s.MmatchIndex[i] = 0
	}
	s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
	s.PendingReconfig = s.GetLastCommandIndex()
}

func (s *State) UpdateCommitIndex() {
	if s.State == statepb.Leader {
		m := make([]uint64, len(s.MmatchIndex))
		copy(m, s.MmatchIndex)
		s.MmatchIndex = m
		s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
		majority := s.Quorum.CommitIndex(&s.MmatchIndex, &s.Peers)
		if majority > s.CommitIndex {
			old_commit := s.CommitIndex
			command := s.GetCommand(majority)
			if command.CommandTerm == s.CurrentTerm {
				s.CommitIndex = max(command.CommandIndex, s.CommitIndex)
				for i := old_commit; i <= s.CommitIndex; i++ {
					c := s.GetCommand(i)
					if c.Command.Type == statepb.EntryConfChange {
						//if this is a single change or leave joint, apply it asap.
						if len(c.Command.ConfChange) <= 1 {
							s.ApplyReconfChange(c.Command.ConfChange)
						}
					}
				}
			}
		}
	}
}

func (s *State) Equal(s1 *State, merge bool) bool {
	if s.Me != s1.Me {
		return false
	}
	if s.State != s1.State {
		return false
	}
	if s.CurrentTerm != s1.CurrentTerm {
		return false
	}
	if s.VotedFor != s1.VotedFor {
		return false
	}
	if &(s.Log[0]) != &(s1.Log[0]) {
		if len(s.Log) != len(s1.Log) {
			return false
		}
		for i, v := range s.Log {
			if !reflect.DeepEqual(v.Command.ReqID, s1.Log[i].Command.ReqID) {
				return false
			}
		}
	}
	if s.CommitIndex != s1.CommitIndex {
		return false
	}

	if s.State == statepb.Candidate {
		if &(s.Votes[0]) != &(s1.Votes[0]) {
			if !reflect.DeepEqual(s.Votes, s1.Votes) {
				return false
			}
		}
	}
	if s.State == statepb.Leader {
		if &(s.MmatchIndex[0]) != &(s1.MmatchIndex[0]) {
			if merge {
				if s.Quorum.CommitIndex(&s.MmatchIndex, &s.Peers) != s1.Quorum.CommitIndex(&s1.MmatchIndex, &s1.Peers) {
					return false
				}
			} else {
				if !reflect.DeepEqual(s.MmatchIndex, s1.MmatchIndex) {
					return false
				}
			}
		}
	}
	return true
}

func (s *State) LogGC() {
	if len(s.Log) <= 100 {
		return
	}
	local_index := s.GetCurrentIndex(s.CommitIndex)
	if local_index == TRUNCATED {
		log.Fatalf("The last committed entry has been truncated, incorrect! commit index: %+v, first index: %+v\n", s.CommitIndex, s.GetFirstCommandIndex())
	}
	if local_index == NONEXIST {
		log.Fatal("The commit index is larger than the end of the log!\n")
	}
	if s.CommitIndex < s.GetLastCommandIndex()-100 {
		s.Log = s.Log[local_index:]
	} else {
		s.Log = s.Log[s.GetCurrentIndex(s.GetLastCommandIndex()-100):]
	}
}

func (s *State) SearchGet(r messagepb.Message) bool {
	last_commit_value_match := true
	for i := s.GetFirstCommandIndex(); i <= s.CommitIndex; i++ {
		command := s.GetCommand(i)
		op := command.Command
		if op.Type != statepb.EntryKVPut || !bytes.Equal(op.Key, r.Key) {
			continue
		}

		if bytes.Equal(op.Value, r.Value) {
			last_commit_value_match = true
		} else {
			last_commit_value_match = false
		}
	}

	return last_commit_value_match
}

// hack function
func (s *State) HackEntries(entries []statepb.Command) {
	l := make([]statepb.Command, len(s.Log))
	copy(l, s.Log)
	s.Log = l
	for i := 0; i < len(entries); i++ {
		cmd := entries[i]
		if s.GetFirstCommandIndex() > cmd.CommandIndex {
			continue
		}
		if s.GetLastCommandIndex() < cmd.CommandIndex {
			if s.GetLastCommandIndex() == cmd.CommandIndex-1 {
				s.Log = append(s.Log, cmd)
				continue
			} else {
				s.Log = append(s.Log, statepb.Command{CommandTerm: s.GetLastCommandTerm(), CommandIndex: s.GetLastCommandIndex() + 1})
			}
		} else {
			local_index := s.GetCurrentIndex(cmd.CommandIndex)
			s.Log[local_index] = cmd
		}
	}
	mi := make([]uint64, len(s.MmatchIndex))
	copy(mi, s.MmatchIndex)
	s.MmatchIndex = mi
	s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
}

func (s *State) InsertEntries(entry statepb.Command) {
	l := make([]statepb.Command, len(s.Log)+1)
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
	s.Log = l
}

// helper function
func Equal(arg1 uint64, arg2 uint64) bool {
	if arg1 == TRUNCATED || arg2 == TRUNCATED {
		return true
	}
	if arg1 == NONEXIST || arg2 == NONEXIST {
		return false
	} else {
		return arg1 == arg2
	}

}

func SliceEqual(arg1 []statepb.Command, arg2 []statepb.Command) bool {
	if len(arg1) != len(arg2) {
		return false
	}
	length := len(arg1)
	for index := 0; index < length; index++ {
		c1 := arg1[index]
		c2 := arg2[index]
		if c1.CommandIndex == NONEXIST || c2.CommandIndex == NONEXIST {
			return false
		}
		if c1.CommandIndex == TRUNCATED || c2.CommandIndex == TRUNCATED {
			continue
		}
		if !(c1.CommandIndex == c2.CommandIndex && c1.CommandTerm == c2.CommandTerm && reflect.DeepEqual(c1.Command, c2.Command)) {
			return false
		}
	}
	return true
}

func (s *State) GetCommitIndex() uint64 {
	return s.CommitIndex
}

func (s *State) GetFirstCommandIndex() uint64 {
	return s.Log[0].CommandIndex
}

func (s *State) GetLastCommandIndex() uint64 {
	return s.Log[len(s.Log)-1].CommandIndex
}

func (s *State) GetLastCommandTerm() uint64 {
	return s.Log[len(s.Log)-1].CommandTerm
}

func (s *State) GetCurrentIndex(commandIndex uint64) uint64 {
	if len(s.Log) == 0 {
		return NONEXIST
	}
	first_command_index := s.Log[0].CommandIndex
	if first_command_index > commandIndex {
		return TRUNCATED
	} else if commandIndex-first_command_index > uint64(len(s.Log)-1) {
		return NONEXIST
	}
	return commandIndex - first_command_index
}

func (s *State) GetCommand(commandIndex uint64) statepb.Command {
	localIndex := s.GetCurrentIndex(commandIndex)
	if localIndex == TRUNCATED {
		return statepb.Command{CommandIndex: TRUNCATED, CommandTerm: TRUNCATED}
	} else if localIndex == NONEXIST {
		return statepb.Command{CommandIndex: NONEXIST, CommandTerm: NONEXIST}
	}
	return s.Log[localIndex]
}

func (s *State) GetCommandTerm(commandIndex uint64) uint64 {
	return s.GetCommand(commandIndex).CommandTerm
}

func (s *State) GetLogSlice(prevIndex uint64, length uint64) []statepb.Command {
	var result []statepb.Command
	var i uint64
	for i = 1; i <= length; i++ {
		result = append(result, s.GetCommand(prevIndex+i))
	}
	return result
}

func (s *State) ReceiveVotes(from uint64) {
	// fmt.Printf("peers are: %+v, from: %+v\n", s.Peers, from)
	s.Votes[inSlice(s.Peers, from)] = true
}

func (s *State) entryInLog(args statepb.Op) (bool, statepb.Command) {
	for _, command := range s.Log {
		if reflect.DeepEqual(args, command.Command) {
			return true, command
		}
	}
	return false, statepb.Command{}
}

func (s *State) appendEntries(entries []statepb.Command) {
	l := make([]statepb.Command, len(s.Log))
	copy(l, s.Log)
	s.Log = l
	for _, command := range entries {
		current_index := s.GetCurrentIndex(command.CommandIndex)
		if current_index == TRUNCATED {
			continue
		}
		if current_index == NONEXIST {
			s.Log = append(s.Log, command)
			continue
		}
		s.Log[current_index] = command
	}
}

func (s *State) QuorumActive(connections []uint64) bool {
	count := 0
	for _, connected_peer := range connections {
		if inSlice(s.Peers, connected_peer) != -1 {
			count = count + 1
		}
	}
	return count > len(s.Peers)/2
}

func (s *State) IndexValid(valid_indice []uint64) bool {
	for _, i := range valid_indice {
		if i <= s.CommitIndex {
			return true
		}
	}
	return false
}

// helper function
func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

// func findMajority(array []uint64) uint64 {
// 	cp := make([]uint64, len(array))
// 	copy(cp, array)
// 	sort.Slice(cp, func(i, j int) bool {
// 		return cp[i] < cp[j]
// 	})
// 	length := len(cp)
// 	return cp[(length-1)/2]
// }

func inSlice(s []uint64, e uint64) int {
	if s == nil {
		return -1
	}
	length := len(s)
	for i := 0; i < length; i++ {
		if (s)[i] == e {
			return i
		}
	}
	return -1
}

func rmSlice(s *simquorum.MajorityQuorum, e uint64) {
	index := -1
	for i, v := range *s {
		if v == e {
			index = i
			break
		}
	}
	if index == -1 {
		return
	}
	for i := index + 1; i < len(*s); i++ {
		(*s)[i-1] = (*s)[i]
	}
	(*s) = (*s)[:len(*s)-1]
}
