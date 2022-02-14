package simulation

import (
	"bytes"
	"log"
	"math"
	"reflect"
	"sort"
	"sync"

	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/raft/raftpb"
	"go.etcd.io/etcd/v3/simulationpb"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type StateType int32

const (
	Leader    StateType = 0
	Follower  StateType = 1
	Candidate StateType = 2
)

const (
	TRUNCATED = math.MaxInt64
	NONEXIST  = math.MaxInt64 - 1
)

const NOONE = math.MaxInt64 - 1

type EntryType int32

const (
	EntryNormal     EntryType = 0
	EntryKVPut      EntryType = 1
	EntryKVRO       EntryType = 2
	EntryConfChange EntryType = 3
)

type RespType int32

const (
	ReadResp   RespType = 0
	NormalResp RespType = 1
	ConfChange RespType = 2
)

type RequestVoteArgs struct {
	From          uint64
	mterm         uint64
	mlastLogIndex uint64
	mlastLogTerm  uint64
}

type RequestVoteReply struct {
	From         uint64
	mterm        uint64
	mvoteGranted bool
}

type AppendEntriesArgs struct {
	From          uint64
	mterm         uint64
	mprevLogIndex uint64
	mprevLogTerm  uint64
	mentries      []Command
	mcommitIndex  uint64
}

type AppendEntriesReply struct {
	From        uint64
	mterm       uint64
	msuccess    bool
	mmatchIndex uint64
}

type Command struct {
	CommandIndex uint64
	Command      Op
	CommandTerm  uint64
}

type Op struct {
	Type  EntryType
	Key   []byte
	Value []byte
	Data  []byte
}

type Resp struct {
	Type  RespType
	Index uint64
	Key   []byte
	Value []byte
}

type GetReply struct {
	Success bool
	Value   string
}

type PutAppendReply struct {
	Success bool
}

type ClientEntry struct {
	Req   interface{}
	Index uint64
}

//
// A Go object implementing a single Raft peer.
//
type Simulation struct {
	mu          sync.Mutex // Lock to protect shared access to this peer's state
	me          uint64
	State       StateType
	CurrentTerm uint64
	VotedFor    uint64
	Votes       []bool
	Log         []Command
	CommitIndex uint64
	mmatchIndex []uint64
	peers       []uint64
	Alive       []bool
	MsgCh       chan simulationpb.Msg

	// RaftBuffer   []interface{}
	ClientBuffer []ClientEntry
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	alive := make([]bool, total)
	for i := 0; i < total; i++ {
		votes[i] = false
		alive[i] = false
	}
	matchIndex := make([]uint64, total)
	for i := 0; i < total; i++ {
		matchIndex[i] = 0
	}
	//hack the initial state
	log := make([]Command, 4)
	log[0] = Command{CommandIndex: 0}
	log[3] = Command{CommandIndex: 3, CommandTerm: 1}
	// log[8] = Command{CommandIndex: 8}

	sl := new(Simulation)
	sl.me = id
	sl.State = Follower
	sl.CurrentTerm = 1
	sl.VotedFor = NOONE
	sl.Votes = votes
	sl.Alive = alive
	sl.Log = log
	sl.CommitIndex = 0
	sl.mmatchIndex = matchIndex
	sl.peers = peers
	sl.MsgCh = make(chan simulationpb.Msg)

	// sl.RaftBuffer = make([]interface{}, 0)
	sl.ClientBuffer = make([]ClientEntry, 0)

	go func() {
		for {
			select {
			case msg := <-sl.MsgCh:
				if msg.Send {
					if msg.Type == simulationpb.ClientMsg {
						sl.CheckClientReplyMessage(msg.Msg, msg.Req)
					} else {
						sl.CheckRaftMessage(msg.Msg.(raftpb.Message), msg.Req)
					}
				} else {
					if msg.Type == simulationpb.ClientMsg {
						sl.HandleClientRequest(msg.Msg)
					} else {
						sl.HandleRaftMessages(msg.Msg.(raftpb.Message))
					}
				}
			}
		}
	}()
	return sl
}

func NewSL(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	alive := make([]bool, total)
	for i := 0; i < total; i++ {
		votes[i] = false
		alive[i] = false
	}
	matchIndex := make([]uint64, total)
	for i := 0; i < total; i++ {
		matchIndex[i] = 0
	}
	//hack the initial state
	log := make([]Command, 9)
	log[0] = Command{CommandIndex: 0}
	log[8] = Command{CommandIndex: 8}
	return &Simulation{
		me:          id,
		State:       Follower,
		CurrentTerm: 0,
		VotedFor:    NOONE,
		Votes:       votes,
		Alive:       alive,
		Log:         log,
		CommitIndex: 0,
		mmatchIndex: matchIndex,
		peers:       peers,
	}
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

func SliceEqual(arg1 []Command, arg2 []Command) bool {
	////////fmt.Printf("print commands: %v, %v\n", arg1, arg2)
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
		if !(c1.CommandIndex == c2.CommandIndex && c1.CommandTerm == c2.CommandTerm && OpEqual(c1.Command, c2.Command)) {
			return false
		}
	}
	return true
}

func (s *Simulation) getFirstCommandIndex() uint64 {
	return s.Log[0].CommandIndex
}

func (s *Simulation) getLastCommandIndex() uint64 {
	return s.Log[len(s.Log)-1].CommandIndex
}

func (s *Simulation) getLastCommandTerm() uint64 {
	return s.Log[len(s.Log)-1].CommandTerm
}

func (s *Simulation) getCurrentIndex(commandIndex uint64) uint64 {
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

func (s *Simulation) getCommand(commandIndex uint64) Command {
	localIndex := s.getCurrentIndex(commandIndex)
	if localIndex == TRUNCATED {
		return Command{CommandIndex: TRUNCATED, CommandTerm: TRUNCATED}
	} else if localIndex == NONEXIST {
		return Command{CommandIndex: NONEXIST, CommandTerm: NONEXIST}
	}
	return s.Log[localIndex]
}

func (s *Simulation) getCommandTerm(commandIndex uint64) uint64 {
	return s.getCommand(commandIndex).CommandTerm
}

func (s *Simulation) getLogSlice(prevIndex uint64, length uint64) []Command {
	var result []Command
	var i uint64
	for i = 1; i <= length; i++ {
		result = append(result, s.getCommand(prevIndex+i))
	}
	return result
}

func (s *Simulation) entryInLog(args Op) (bool, Command) {
	for _, command := range s.Log {
		if OpEqual(command.Command, args) {
			return true, command
		}
	}
	return false, Command{}
}

func (s *Simulation) InsertEmtpy(in_cmd Command) {
	index := in_cmd.CommandIndex
	if index < s.getFirstCommandIndex() {
		log.Fatal("the log has already been truncated, cannot insert the emtpy entry\n")
		return
	}
	if index > s.getLastCommandIndex()+1 {
		log.Fatal("the log is too short to append the empty entry\n")
		return
	}
	if index == s.getLastCommandIndex()+1 {
		s.Log = append(s.Log, in_cmd)
		return
	}

	local_index := s.getCurrentIndex(index)
	for i := 0; i < len(s.Log); i++ {
		if i < int(local_index) {
			continue
		}
		cmd := s.Log[i]
		s.Log[i] = Command{CommandTerm: cmd.CommandTerm, CommandIndex: cmd.CommandIndex + 1, Command: cmd.Command}
	}
	s.Log = append(append(s.Log[:local_index], in_cmd), s.Log[local_index:]...)
	return
}

func (s *Simulation) logMatch(l []Command) bool {
	for i := 0; i < len(l); i++ {
		cmd := l[i]
		local_cmd := s.getCommand(cmd.CommandIndex)
		if local_cmd.CommandIndex == TRUNCATED {
			continue
		}
		if local_cmd.CommandIndex == NONEXIST {
			if !(cmd.Command.Data == nil && cmd.CommandIndex == s.getLastCommandIndex()+1) {
				return false
			}
			s.InsertEmtpy(cmd)
			continue
		}
		if reflect.DeepEqual(cmd, local_cmd) {
			continue
		}
		if cmd.Command.Data != nil {
			return false
		}
		s.InsertEmtpy(cmd)
	}
	return true
}

//func (s *Simulation) Get(key string, commandIndex int) (bool, string) {
//	found := false
//	value := ""
//	for _, command := range s.Log {
//		if command.CommandIndex > commandIndex {
//			break
//		}
//		if command.Command.Key == key && command.Command.Op == "PUT" {
//			found = true
//			value = command.Command.Value
//		}
//	}
//	return found, value
//}

func (s *Simulation) appendEntries(entries []Command) {
	for _, command := range entries {
		current_index := s.getCurrentIndex(command.CommandIndex)
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

//Interpreter: interpret the message to simulation

func CommandInterpreter(command raftpb.Entry) Command {
	//var req *pb.InternalRaftRequest
	if command.Data == nil {
		return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryNormal, Data: command.Data}}
	}

	req := new(pb.InternalRaftRequest)
	err := req.Unmarshal(command.Data)
	if err != nil {
		//log.Fatalf("command unmarshal failed: %+v\n", command)
		return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryNormal, Data: command.Data}}
	}
	if req.Put != nil {
		data, _ := (req.Put).Marshal()
		return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryKVPut, Key: req.Put.Key, Value: req.Put.Value, Data: data}}
	}
	return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryNormal, Data: command.Data}}
}

func CommandsInterpreter(commands []raftpb.Entry) []Command {
	var result []Command
	for _, command := range commands {
		result = append(result, CommandInterpreter(command))
	}
	return result
}

//func GetRequestInterpreter(args *Common.GetArgs) Op {
//	return Op{Op: "GET", Key: args.Key, ClientId: args.ClientId, ReqId: args.ReqId}
//}
//
func PutRequestInterpreter(req *pb.PutRequest) Op {
	r := pb.InternalRaftRequest{Put: req}
	data, err := r.Marshal()
	if err != nil {
		log.Fatalf("put request interpreter marshal failed: %+v\n", req)
	}
	return Op{Type: EntryKVPut, Key: req.Key, Value: req.Value, Data: data}
}

func ClientRequestInterpreter(req interface{}) Op {
	switch req.(type) {
	case *pb.RangeRequest:
		return Op{Type: EntryKVRO, Key: req.(*pb.RangeRequest).Key}
	case *pb.PutRequest:
		return PutRequestInterpreter(req.(*pb.PutRequest))
	default:
		return Op{Type: EntryNormal}
	}
}

func ClientResponseInterpreter(index uint64, resp interface{}) Resp {
	switch resp.(type) {
	case *pb.RangeResponse:
		return Resp{Type: ReadResp, Index: index, Key: resp.(*pb.RangeResponse).Kvs[0].Key, Value: resp.(*pb.RangeResponse).Kvs[0].Value}
	default:
		return Resp{Type: NormalResp, Index: index}
	}
}

//func GetReplyInterpreter(args *Common.GetReply) GetReply {
//	return GetReply{Value: args.Value, Success: args.Err == Common.OK}
//}
//
//func PutReplyInterpreter(args *Common.PutAppendReply) PutAppendReply {
//	return PutAppendReply{Success: args.Err == Common.OK}
//}

//func RequestVoteRequestInterPreter(args *Common.RequestVoteArgs) RequestVoteArgs {
//	return RequestVoteArgs{From: args.From, mterm: args.Term, msource: args.CandidateId, mlastLogIndex: args.LastLogIndex, mlastLogTerm: args.LastLogTerm}
//}
//
//func AppendEntryRequestInterPreter(args *Common.AppendEntriesArgs) AppendEntriesArgs {
//	return AppendEntriesArgs{From: args.From, mterm: args.Term, msource: args.LeaderId, mprevLogIndex: args.PrevLogIndex, mprevLogTerm: args.PrevLogTerm, mentries: CommandsInterpreter(args.Entries), mcommitIndex: args.LeaderCommit}
//}
//
//func RequestVoteReplyInterpreter(reply *Common.RequestVoteReply) RequestVoteReply {
//	return RequestVoteReply{From: reply.From, mterm: reply.Term, mvoteGranted: reply.VoteGranted}
//}
//
//func AppendEntryReplyInterpreter(reply *Common.AppendEntriesReply) AppendEntriesReply {
//	return AppendEntriesReply{From: reply.From, mterm: reply.Term, msuccess: reply.Success}
//}

func (s *Simulation) MsgInterpreter(msg raftpb.Message) interface{} {
	switch msg.Type {
	case raftpb.MsgApp:
		return AppendEntriesArgs{From: msg.From, mterm: msg.Term, mprevLogIndex: msg.Index, mprevLogTerm: msg.LogTerm, mentries: CommandsInterpreter(msg.Entries), mcommitIndex: msg.Commit}
	case raftpb.MsgAppResp:
		return AppendEntriesReply{From: msg.From, mterm: msg.Term, msuccess: !msg.Reject, mmatchIndex: msg.Index}
	case raftpb.MsgVote:
		return RequestVoteArgs{From: msg.From, mterm: msg.Term, mlastLogIndex: msg.Index, mlastLogTerm: msg.LogTerm}
	case raftpb.MsgVoteResp:
		return RequestVoteReply{From: msg.From, mterm: msg.Term, mvoteGranted: !msg.Reject}
	case raftpb.MsgHeartbeat:
		return AppendEntriesArgs{From: msg.From, mterm: msg.Term, mprevLogIndex: s.getLastCommandIndex(), mprevLogTerm: s.getLastCommandTerm(), mentries: []Command{}, mcommitIndex: msg.Commit}
	case raftpb.MsgHeartbeatResp:
		return AppendEntriesReply{From: msg.From, mterm: msg.Term, msuccess: !msg.Reject, mmatchIndex: msg.Index}
	}
	return nil
}

//call From the upper layer
//checker function: check whether the messages are valid before actually send them out
func (s *Simulation) CheckRaftMessage(msg raftpb.Message, req interface{}) {
	valid := true
	switch msg.Type {
	case raftpb.MsgVote:
		valid = s.RequestVoteRequestChecker(s.MsgInterpreter(msg).(RequestVoteArgs))
	case raftpb.MsgApp:
		valid = s.AppendEntryRequestChecker(s.MsgInterpreter(msg).(AppendEntriesArgs))
	case raftpb.MsgHeartbeat:
		valid = s.AppendEntryRequestChecker(s.MsgInterpreter(msg).(AppendEntriesArgs))
	case raftpb.MsgVoteResp:
		// r := s.HandleRaftReqMessages(req.(raftpb.Message)).(RequestVoteReply)
		valid = s.RequestVoteReplyChecker(s.MsgInterpreter(msg).(RequestVoteReply), s.HandleRaftReqMessages(req.(raftpb.Message)).(RequestVoteReply))
		// if !valid {
		// 	log.Fatalf("check request message error! server: %d, args: %+v\n local state: %+v\n, reply_impl: %+v\n, reply_sl: %+v\n", s.me, req, s, msg, r)
		// }
	case raftpb.MsgAppResp:
		valid = s.AppendEntryReplyChecker(s.MsgInterpreter(msg).(AppendEntriesReply), s.HandleRaftReqMessages(req.(raftpb.Message)).(AppendEntriesReply))
	case raftpb.MsgHeartbeatResp:
		valid = s.AppendEntryReplyChecker(s.MsgInterpreter(msg).(AppendEntriesReply), s.HandleRaftReqMessages(req.(raftpb.Message)).(AppendEntriesReply))
	}
	if !valid {
		log.Fatalf("check request message error! server: %d, msg: %+v\n req: %+v\n local state: %+v\n", s.me, msg, req, s)
	}
}

func (s *Simulation) CheckClientReplyMessage(reply interface{}, req interface{}) {
	find, index := s.searchInClientBuffer(req)
	if !find {
		log.Fatalf("%d haven't received the client req before\n", s.me)
	}
	valid := s.ClientReplyChecker(ClientResponseInterpreter(index, reply))
	if !valid {
		log.Fatalf("check reply message error! server: %d, index: %d, reply: %+v\n local state: %+v\n", s.me, index, reply, s)
	}
}

//handler function: update the simulation state once receives the requests

func (s *Simulation) HandleClientRequest(req interface{}) {
	s.ClientBuffer = append(s.ClientBuffer, ClientEntry{Req: req, Index: s.ClientRequestHandler(ClientRequestInterpreter(req))})
	log.Printf("!!!!!!!client req: %+v\n local state: %+v\n", req, s)
	return
}

func (s *Simulation) HandleRaftReqMessages(msg raftpb.Message) interface{} {
	s.ConnectionAlive(msg)
	switch msg.Type {
	case raftpb.MsgProp:
		if msg.Entries[0].Type != raftpb.EntryNormal {
			log.Fatalf("the prop type is not normal\n")
		}
		s.ClientRequestHandler(Op{Type: EntryNormal, Data: msg.Entries[0].Data})
	case raftpb.MsgVote:
		r := s.RequestVoteRequestHandler(s.MsgInterpreter(msg).(RequestVoteArgs))
		log.Printf("receives MsgVote: %+v, after interpreter: %+v, reply: %+v\n", msg, s.MsgInterpreter(msg).(RequestVoteArgs), r)
		return r
		// s.RaftBuffer = append(s.RaftBuffer, r)
	case raftpb.MsgApp:
		r := s.AppendEntryRequestHandler(s.MsgInterpreter(msg).(AppendEntriesArgs))
		log.Printf("receives MsgApp: %+v, after interpreter: %+v, reply: %+v\n", msg, s.MsgInterpreter(msg).(AppendEntriesArgs), s.AppendEntryRequestHandler(s.MsgInterpreter(msg).(AppendEntriesArgs)))
		return r
		// s.RaftBuffer = append(s.RaftBuffer, r)
	case raftpb.MsgHeartbeat:
		r := s.AppendEntryRequestHandler(s.MsgInterpreter(msg).(AppendEntriesArgs))
		return r
	}
	return nil
}

func (s *Simulation) HandleRaftMessages(msg raftpb.Message) interface{} {
	s.ConnectionAlive(msg)
	switch msg.Type {
	case raftpb.MsgVoteResp:
		s.RequestVoteReplyHandler(s.MsgInterpreter(msg).(RequestVoteReply))
	case raftpb.MsgAppResp:
		s.AppendEntryReplyHandler(s.MsgInterpreter(msg).(AppendEntriesReply))
	case raftpb.MsgHeartbeatResp:
		s.AppendEntryReplyHandler(s.MsgInterpreter(msg).(AppendEntriesReply))
	}
	return nil
}

// func (s *Simulation) HandleRaftRequestMessage(req raftpb.Message) {
// 	switch req.Type {
// 	case raftpb.MsgVote:
// 		s.RequestVoteRequestHandler(s.MsgInterpreter(req).(RequestVoteArgs))
// 	case raftpb.MsgApp:
// 		s.AppendEntryRequestHandler(s.MsgInterpreter(req).(AppendEntriesArgs))
// 	case raftpb.MsgHeartbeat:
// 		s.AppendEntryRequestHandler(s.MsgInterpreter(req).(AppendEntriesArgs))
// 	}
// }

// func (s *Simulation) HandleRaftReplyMessage(reply raftpb.Message) {
// 	switch reply.Type {
// 	case raftpb.MsgVoteResp:
// 		s.RequestVoteReplyHandler(s.MsgInterpreter(reply).(RequestVoteReply))
// 	case raftpb.MsgAppResp:
// 		s.AppendEntryReplyHandler(s.MsgInterpreter(reply).(AppendEntriesReply))
// 	case raftpb.MsgHeartbeatResp:
// 		s.AppendEntryReplyHandler(s.MsgInterpreter(reply).(AppendEntriesReply))
// 	}
// }

func (s *Simulation) ConnectionAlive(msg raftpb.Message) {
	s.Alive[inSlice(s.peers, msg.From)] = true
}

//checker: check whether the message is correct

func (s *Simulation) RequestVoteRequestChecker(args RequestVoteArgs) bool {
	if s.State != Candidate {
		if !s.TimeoutChecker() {
			return false
		}
		s.Candidate()
	}

	//hack the initial state
	if args.mlastLogIndex <= 8 {
		return true
	}

	return args.From == s.me && s.State == Candidate && args.mterm == s.CurrentTerm && args.mlastLogIndex == s.getLastCommandIndex() && args.mlastLogTerm == s.getLastCommandTerm()
}

func (s *Simulation) AppendEntryRequestChecker(args AppendEntriesArgs) bool {
	if s.State != Leader {
		if !s.testLeader() {
			return false
		}
		s.Leader()
	}

	//hack the initial state
	if args.mprevLogIndex <= 8 {
		s.hackEntries(args.mentries)
		return true
	}

	state_check := (s.State == Leader) && (args.mterm == s.CurrentTerm) && (args.From == s.me) && (args.mcommitIndex <= s.CommitIndex)
	// params_checker := Equal(args.mprevLogTerm, s.getCommandTerm(args.mprevLogIndex)) && SliceEqual(args.mentries, s.getLogSlice(args.mprevLogIndex, uint64(len(args.mentries))))
	params_checker := Equal(args.mprevLogTerm, s.getCommandTerm(args.mprevLogIndex)) && s.logMatch(args.mentries)
	//////fmt.Printf("request args: %+v, local state: %+v\n", args, s)
	//////fmt.Printf("state_check: %+v, params_check: %+v\n", state_check, SliceEqual(args.mentries, s.getLogSlice(args.mprevLogIndex, len(args.mentries))))
	return args.From == s.me && state_check && params_checker
}

func (s *Simulation) TimeoutChecker() bool {
	return s.State == Follower || s.State == Candidate
}

func (s *Simulation) BecomeLeaderChecker() bool {
	return s.testLeader()
}

func (s *Simulation) RequestVoteReplyChecker(r_exec RequestVoteReply, r_sl RequestVoteReply) bool {
	// return s.searchInRaftBuffer(r_exec)
	r := (r_exec == r_sl)
	if !r {
		log.Printf("!!!!!!!!!requestvotereplychecker failed. exec: %+v\n sl: %+v\n", r_exec, r_sl)
	}
	return r
}

func (s *Simulation) AppendEntryReplyChecker(r_exec AppendEntriesReply, r_sl AppendEntriesReply) bool {
	// return s.searchInRaftBuffer(r_exec)
	r := (r_exec.From == r_sl.From) && (r_exec.mterm == r_sl.mterm) && (r_exec.msuccess == r_sl.msuccess) && (r_exec.mmatchIndex <= r_sl.mmatchIndex)
	if !r {
		log.Printf("!!!!!!!!!appendentryreplychecker failed. exec: %+v\n sl: %+v\n", r_exec, r_sl)
	}
	return r
}

//func (s *Simulation) GetReplyChecker(args Op, reply GetReply) bool {
//	found, command := s.entryInLog(args)
//	key_in_log, value := s.Get(args.Key, command.CommandIndex)
//	commit := s.CommitIndex >= command.CommandIndex
//	return s.inConfig() && (!reply.Success || !found || !key_in_log || value == reply.Value && commit)
//}
//

func (s *Simulation) ClientReplyChecker(resp Resp) bool {
	switch resp.Type {
	case ReadResp:
		command := s.getCommand(resp.Index)
		if command.CommandIndex == TRUNCATED {
			return true
		}
		return s.QuorumActive() && s.searchGet(resp)
	case NormalResp:
		index := resp.Index
		return s.CommitIndex >= index
	default:
		return true
	}
}

func (s *Simulation) searchGet(r Resp) bool {
	first_valid_commit := -1
	for i := s.getFirstCommandIndex(); i <= s.CommitIndex; i++ {
		command := s.getCommand(i)
		op := command.Command
		if op.Type != EntryKVPut || !bytes.Equal(op.Key, r.Key) {
			continue
		}
		if i <= r.Index {
			first_valid_commit = int(i)
		} else {
			if bytes.Equal(op.Value, r.Value) {
				return true
			}
		}
	}

	if first_valid_commit == -1 {
		return true
	}

	cd := s.getCommand(uint64(first_valid_commit))
	return bytes.Equal(cd.Command.Value, r.Value)
}

func (s *Simulation) QuorumActive() bool {
	s.Alive[inSlice(s.peers, s.me)] = true
	nums := 0
	for i := 0; i < len(s.peers); i++ {
		if s.Alive[i] {
			nums++
		}
	}
	return nums > len(s.peers)/2
}

//Handler:
func (s *Simulation) ClientRequestHandler(args Op) uint64 {
	log.Printf("!!!!!!!!client req: %+v\n", args)
	if s.State == Leader {
		if args.Type == EntryKVRO {
			for i := 0; i < len(s.peers); i++ {
				s.Alive[i] = false
			}
			return s.CommitIndex
		} else {
			index := s.getLastCommandIndex() + 1
			command := Command{Command: args, CommandIndex: index, CommandTerm: s.CurrentTerm}
			s.Log = append(s.Log, command)
			s.mmatchIndex[inSlice(s.peers, s.me)] = s.getLastCommandIndex()
			return index
		}
	}
	return math.MaxInt64
}

func (s *Simulation) RequestVoteRequestHandler(args RequestVoteArgs) RequestVoteReply {
	r := RequestVoteReply{From: s.me}
	if s.DropStaleResponse(args.mterm) {
		r.mterm = s.CurrentTerm
		r.mvoteGranted = false
		return r
	}
	s.UpdateTerm(args.mterm)
	logOK := (args.mlastLogTerm > s.getLastCommandTerm()) || (args.mlastLogTerm == s.getLastCommandTerm() && args.mlastLogIndex >= s.getLastCommandIndex())
	grant := (args.mterm == s.CurrentTerm) && logOK && (s.VotedFor == NOONE || s.VotedFor == args.From)
	if grant {
		s.VotedFor = args.From
	}
	r.mterm = s.CurrentTerm
	r.mvoteGranted = grant
	log.Printf("server %d receives vote request, args: %+v\n, local: %+v\n, reply: %+v\n", s.me, args, s, r)
	return r
}

func (s *Simulation) AppendEntryRequestHandler(args AppendEntriesArgs) AppendEntriesReply {
	r := AppendEntriesReply{From: s.me}
	if s.DropStaleResponse(args.mterm) {
		r.mterm = s.CurrentTerm
		r.msuccess = false
		return r
	}
	s.UpdateTerm(args.mterm)
	logOK := (args.mprevLogIndex == 0) || (args.mprevLogIndex > 0 && args.mprevLogIndex <= s.getLastCommandIndex() && Equal(args.mprevLogTerm, s.getCommandTerm(args.mprevLogIndex)))

	//hack the initial state:
	// if args.mprevLogIndex <= 8 {
	// 	logOK = true
	// }

	if args.mterm == s.CurrentTerm && s.State == Follower && !logOK {
		r.mterm = s.CurrentTerm
		r.msuccess = false
		return r
	}
	if args.mterm == s.CurrentTerm && s.State == Candidate {
		s.State = Follower
	}
	if args.mterm == s.CurrentTerm && s.State == Follower && logOK {
		s.appendEntries(args.mentries)
		s.CommitIndex = max(s.CommitIndex, args.mcommitIndex)
	}
	r.mterm = s.CurrentTerm
	r.msuccess = (args.mterm == s.CurrentTerm) && (s.State == Follower) && logOK
	r.mmatchIndex = args.mprevLogIndex + uint64(len(args.mentries))
	return r
}

func (s *Simulation) RequestVoteReplyHandler(reply RequestVoteReply) {
	if s.DropStaleResponse(reply.mterm) {
		return
	}
	s.UpdateTerm(reply.mterm)

	if reply.mvoteGranted {
		s.Votes[inSlice(s.peers, reply.From)] = true
	}
	return
}

func (s *Simulation) AppendEntryReplyHandler(reply AppendEntriesReply) {
	////fmt.Printf("appendentry args, %+v, reply: %+v\n", args, reply)
	if s.DropStaleResponse(reply.mterm) {
		return
	}
	s.UpdateTerm(reply.mterm)
	if reply.msuccess {
		s.mmatchIndex[inSlice(s.peers, reply.From)] = max(reply.mmatchIndex, s.mmatchIndex[inSlice(s.peers, reply.From)])
		s.updateCommitIndex()
	}
	return
}

//other functions
func (s *Simulation) DropStaleResponse(mterm uint64) bool {
	if mterm < s.CurrentTerm {
		return true
	}
	return false
}

func (s *Simulation) UpdateTerm(mterm uint64) {
	if mterm > s.CurrentTerm {
		s.CurrentTerm = mterm
		s.Follower()
	}
}

func (s *Simulation) testLeader() bool {
	if s.State != Candidate {
		return false
	}
	votes := 0
	var i int
	for i = 0; i < len(s.peers); i++ {
		if s.Votes[i] == true {
			votes += 1
		}
	}
	if votes > len(s.peers)/2 {
		return true
	}
	return false
}

func (s *Simulation) Candidate() {
	s.State = Candidate
	s.CurrentTerm = s.CurrentTerm + 1
	s.VotedFor = s.me
	for i := 0; i < len(s.peers); i++ {
		s.Votes[i] = false
	}
	s.Votes[inSlice(s.peers, s.me)] = true
}

func (s *Simulation) Follower() {
	s.State = Follower
	s.VotedFor = NOONE
}

func (s *Simulation) Leader() {
	var i int
	s.State = Leader
	for i = 0; i < len(s.peers); i++ {
		s.mmatchIndex[i] = 0
	}
	s.mmatchIndex[inSlice(s.peers, s.me)] = s.getLastCommandIndex()
	////fmt.Printf("server %d becomes leader, state: %+v\n", s.me, s)
}

func (s *Simulation) updateCommitIndex() {
	majority := findMajority(s.mmatchIndex)
	//log.Printf("server tries to update commit index, matchindex: %+v, majority: %d\n", s.mmatchIndex, majority)
	if majority > s.CommitIndex {
		command := s.getCommand(majority)
		if command.CommandTerm == s.CurrentTerm {
			s.CommitIndex = max(command.CommandIndex, s.CommitIndex)
		}
	}
}

// func (s *Simulation) searchInRaftBuffer(e interface{}) bool {
// 	length := len(s.RaftBuffer)
// 	for i := 0; i < length; i++ {
// 		if reflect.DeepEqual(s.RaftBuffer[i], e) {
// 			s.RaftBuffer = append(s.RaftBuffer[:i], s.RaftBuffer[i+1:]...)
// 			return true
// 		}
// 	}
// 	return false
// }

func (s *Simulation) searchInClientBuffer(e interface{}) (bool, uint64) {
	length := len(s.ClientBuffer)
	for i := 0; i < length; i++ {
		if reflect.DeepEqual(s.ClientBuffer[i].Req, e) {
			index := s.ClientBuffer[i].Index
			s.ClientBuffer = append(s.ClientBuffer[:i], s.ClientBuffer[i+1:]...)
			return true, index
		}
	}
	return false, 0
}

//hack function
func (s *Simulation) hackEntries(entries []Command) {
	for i := 0; i < len(entries); i++ {
		cmd := entries[i]
		if s.getFirstCommandIndex() > cmd.CommandIndex {
			continue
		}
		if s.getLastCommandIndex() < cmd.CommandIndex {
			if s.getLastCommandIndex() == cmd.CommandIndex-1 {
				s.Log = append(s.Log, cmd)
				continue
			}
		} else {
			local_index := s.getCurrentIndex(cmd.CommandIndex)
			s.Log[local_index] = cmd
		}
	}
}

//for debug
func (s *Simulation) Dump() {
	log.Printf("simulation state: %+v\n", s)
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

func OpEqual(op1 Op, op2 Op) bool {
	if (op1.Type != op2.Type) || (!bytes.Equal(op1.Key, op2.Key)) || (!bytes.Equal(op1.Value, op2.Value)) || (!bytes.Equal(op1.Data, op2.Data)) {
		return false
	}
	return true
}

func inSlice(s []uint64, e uint64) int {
	length := len(s)
	for i := 0; i < length; i++ {
		if s[i] == e {
			return i
		}
	}
	log.Fatalf("value %d doesn't exist in the array %+v\n", e, s)
	return -1
}
