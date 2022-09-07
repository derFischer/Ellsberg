package state

import (
	"log"
	"math"
	"reflect"
	"sort"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/statepb"
)

const (
	TRUNCATED = math.MaxUint64
	NONEXIST  = math.MaxUint64 - 1
)

const NOONE = math.MaxUint64 - 1

type State struct {
	Me          uint64
	State       statepb.StateType
	CurrentTerm uint64
	VotedFor    uint64
	Votes       []bool
	Log         []statepb.Command
	CommitIndex uint64
	MmatchIndex []uint64
	Peers       []uint64
}

func (s *State) ClientRequestHandler(args statepb.Op) {
	if s.State == statepb.Leader {
		s.Log = append(s.Log, statepb.Command{CommandIndex: s.GetLastCommandIndex() + 1, CommandTerm: s.CurrentTerm, Command: args})
	}
}

func (s *State) RequestVoteRequestHandler(args messagepb.RequestVoteArgs) messagepb.RequestVoteReply {
	r := messagepb.RequestVoteReply{From: s.Me, To: args.From}
	if s.DropStaleResponse(args.MTerm) {
		r.MTerm = s.CurrentTerm
		r.MVoteGranted = false
		return r
	}
	s.UpdateTerm(args.MTerm)
	logOK := (args.MLastLogTerm > s.GetLastCommandTerm()) || (args.MLastLogTerm == s.GetLastCommandTerm() && args.MLastLogIndex >= s.GetLastCommandIndex())
	grant := (args.MTerm == s.CurrentTerm) && logOK && (s.VotedFor == NOONE || s.VotedFor == args.From)
	if grant {
		s.VotedFor = args.From
	}
	r.MTerm = s.CurrentTerm
	r.MVoteGranted = grant
	//fmt.Printf("!!!!!!!!!!!!!!!!!!!REQUEST VOTE REQ REPLY: %+v\n", r)
	return r
}

func (s *State) AppendEntryRequestHandler(args messagepb.AppendEntriesArgs) messagepb.AppendEntriesReply {
	r := messagepb.AppendEntriesReply{From: s.Me, To: args.From}
	if s.DropStaleResponse(args.MTerm) {
		r.MTerm = s.CurrentTerm
		r.MSuccess = false
		return r
	}
	s.UpdateTerm(args.MTerm)
	logOK := (args.MPrevLogIndex == 0) || (args.MPrevLogIndex > 0 && args.MPrevLogIndex <= s.GetLastCommandIndex() && Equal(args.MPrevLogTerm, s.GetCommandTerm(args.MPrevLogIndex)))

	if args.MTerm == s.CurrentTerm && s.State == statepb.Follower && !logOK {
		r.MTerm = s.CurrentTerm
		r.MSuccess = false
		return r
	}
	if args.MTerm == s.CurrentTerm && s.State == statepb.Candidate {
		s.State = statepb.Follower
	}
	if args.MTerm == s.CurrentTerm && s.State == statepb.Follower && logOK {
		s.appendEntries(args.MEntries)
		s.CommitIndex = max(s.CommitIndex, args.MCommitIndex)
	}
	r.MTerm = s.CurrentTerm
	r.MSuccess = (args.MTerm == s.CurrentTerm) && (s.State == statepb.Follower) && logOK
	r.MMatchIndex = args.MPrevLogIndex + uint64(len(args.MEntries))
	return r
}

func (s *State) RequestVoteReplyHandler(reply messagepb.RequestVoteReply) {
	if s.DropStaleResponse(reply.MTerm) {
		return
	}
	s.UpdateTerm(reply.MTerm)

	if reply.MVoteGranted {
		s.ReceiveVotes(reply.From)
	}
	return
}

func (s *State) AppendEntryReplyHandler(reply messagepb.AppendEntriesReply) {
	if s.DropStaleResponse(reply.MTerm) {
		return
	}
	s.UpdateTerm(reply.MTerm)
	if reply.MSuccess {
		s.MmatchIndex[inSlice(s.Peers, reply.From)] = max(reply.MMatchIndex, s.MmatchIndex[inSlice(s.Peers, reply.From)])
		s.updateCommitIndex()
	}
	return
}

//other functions
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
	votes := 0
	var i int
	for i = 0; i < len(s.Peers); i++ {
		if s.Votes[i] == true {
			votes += 1
		}
	}
	if votes > len(s.Peers)/2 {
		return true
	}
	return false
}

func (s *State) Candidate(term uint64) {
	s.State = statepb.Candidate
	s.CurrentTerm = term
	s.VotedFor = s.Me
	for i := 0; i < len(s.Peers); i++ {
		s.Votes[i] = false
	}
	s.ReceiveVotes(s.Me)
}

func (s *State) Follower() {
	s.State = statepb.Follower
	s.VotedFor = NOONE
}

func (s *State) Leader() {
	var i int
	s.State = statepb.Leader
	for i = 0; i < len(s.Peers); i++ {
		s.MmatchIndex[i] = 0
	}
	s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
}

func (s *State) updateCommitIndex() {
	s.MmatchIndex[inSlice(s.Peers, s.Me)] = s.GetLastCommandIndex()
	majority := findMajority(s.MmatchIndex)
	// fmt.Printf("the majority index is :%+v\n", majority)
	if majority > s.CommitIndex {
		command := s.GetCommand(majority)
		if command.CommandTerm == s.CurrentTerm {
			s.CommitIndex = max(command.CommandIndex, s.CommitIndex)
		}
	}
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
	// if s.CommitIndex < s.GetFirstCommandIndex() {
	// 	log.Fatalf("the commit index: %+v, local index: %+v, last index: %+v\n", s.CommitIndex, local_index, s.GetLastCommandIndex())
	// }
	// fmt.Printf("after gc, len of log: %+v\n", s.Log)
}

//hack function
func (s *State) HackEntries(entries []statepb.Command) {
	for i := 0; i < len(entries); i++ {
		cmd := entries[i]
		if s.GetFirstCommandIndex() > cmd.CommandIndex {
			continue
		}
		if s.GetLastCommandIndex() < cmd.CommandIndex {
			if s.GetLastCommandIndex() == cmd.CommandIndex-1 {
				s.Log = append(s.Log, cmd)
				continue
			}
		} else {
			local_index := s.GetCurrentIndex(cmd.CommandIndex)
			s.Log[local_index] = cmd
		}
	}
}

//for debug
func (s *State) Dump() {
	//log.printf("simulation state: %+v\n", s)
}

//helper function
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

//helper function
func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func findMajority(array []uint64) uint64 {
	cp := make([]uint64, len(array))
	copy(cp, array)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i] < cp[j]
	})
	length := len(cp)
	return cp[(length-1)/2]
}

// func OpEqual(op1 statepb.Op, op2 statepb.Op) bool {
// 	if (op1.Type != op2.Type) || (!bytes.Equal(op1.Key, op2.Key)) || (!bytes.Equal(op1.Value, op2.Value)) || (!bytes.Equal(op1.Data, op2.Data)) {
// 		return false
// 	}
// 	return true
// }

func inSlice(s []uint64, e uint64) int {
	if s == nil {
		return -1
	}
	length := len(s)
	for i := 0; i < length; i++ {
		if s[i] == e {
			return i
		}
	}
	return -1
}
