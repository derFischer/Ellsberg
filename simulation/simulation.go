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

type StateType int32

const (
	Leader    StateType = 0
	Follower  StateType = 1
	Candidate StateType = 2
)

const (
	TRUNCATED = math.MaxUint64
	NONEXIST  = math.MaxUint64 - 1
)

const (
	HEARTBEAT = math.MaxUint64
)

const NOONE = math.MaxUint64 - 1

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

//raft msgs
type Ctx interface{}
type Rctx interface{}

type Connection struct {
	ref int
	ids []uint64
}

type RIndex struct {
	ref   int
	index uint64
}

type RequestVoteArgs struct {
	From          uint64
	To            uint64
	mterm         uint64
	mlastLogIndex uint64
	mlastLogTerm  uint64
	ctx           Ctx
}

type RequestVoteReply struct {
	From         uint64
	To           uint64
	mterm        uint64
	mvoteGranted bool
	ctx          Ctx
}

type AppendEntriesArgs struct {
	From          uint64
	To            uint64
	mterm         uint64
	mprevLogIndex uint64
	mprevLogTerm  uint64
	mentries      []Command
	mcommitIndex  uint64
	ctx           Ctx
}

type AppendEntriesReply struct {
	From        uint64
	To          uint64
	mterm       uint64
	msuccess    bool
	mmatchIndex uint64
	ctx         Ctx
}

type ReadIndexArgs struct {
	From uint64
	To   uint64
	rctx Rctx
}

type ReadIndexReply struct {
	From  uint64
	To    uint64
	mterm uint64
	rctx  Rctx
	index uint64
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
	ctxs  []Ctx
}

type GetReply struct {
	Success bool
	Value   string
}

type PutAppendReply struct {
	Success bool
}

type ClientEntry struct {
	ReqID interface{}
	// Req   interface{}
	Index uint64
	Ctxs  []Ctx
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
	Connections map[Ctx]Connection
	MsgCh       chan simulationpb.Msg

	// PendingReadIndex []ReadIndex
	ReadIndexCtx map[Rctx]RIndex

	ClientROBuffer    []ClientEntry
	ClientOtherBuffer []ClientEntry
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	alive := make(map[Ctx]Connection)
	matchIndex := make([]uint64, total)

	for i := 0; i < total; i++ {
		votes[i] = false
		matchIndex[i] = 0
	}

	//hack the initial state
	log := make([]Command, 4)
	log[0] = Command{CommandIndex: 0}
	log[3] = Command{CommandIndex: 3, CommandTerm: 1}

	sl := new(Simulation)
	sl.me = id
	sl.State = Follower
	sl.CurrentTerm = 1
	sl.VotedFor = NOONE
	sl.Votes = votes
	sl.Connections = alive
	sl.Log = log
	sl.CommitIndex = 0
	sl.mmatchIndex = matchIndex
	sl.peers = peers
	sl.MsgCh = make(chan simulationpb.Msg)
	sl.ReadIndexCtx = make(map[Rctx]RIndex)

	sl.ClientROBuffer = make([]ClientEntry, 0)
	sl.ClientOtherBuffer = make([]ClientEntry, 0)

	go func() {
		for {
			select {
			case msg := <-sl.MsgCh:
				if msg.Send {
					if msg.Type == simulationpb.ClientResp {
						sl.CheckClientReplyMessage((msg.Msg).(simulationpb.ClientReply).Reply, (msg.Msg).(simulationpb.ClientReply).ID)
					} else {
						sl.CheckRaftMessage((msg.Msg).(simulationpb.RaftMsg).Msg.(raftpb.Message), (msg.Msg).(simulationpb.RaftMsg).Req)
					}
				} else {
					if msg.Type == simulationpb.ClientRequest {
						sl.HandleClientRequest((msg.Msg).(simulationpb.ClientReq).Req, (msg.Msg).(simulationpb.ClientReq).ID)
					} else {
						sl.HandleRaftMessages((msg.Msg).(simulationpb.RaftMsg).Msg.(raftpb.Message))
					}
				}
			}
		}
	}()
	return sl
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
			//log.printf("!!!!!!!!!!!!!!!!!!log doesnt' match! exec: %+v, simulation: %+v\n", cmd, local_cmd)
			return false
		}
		s.InsertEmtpy(cmd)
	}
	return true
}

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
	if command.Data == nil {
		return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryNormal, Data: command.Data}}
	}

	req := new(pb.InternalRaftRequest)
	err := req.Unmarshal(command.Data)
	if err != nil {
		return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryNormal, Data: command.Data}}
	}
	if req.Put != nil {
		// data, _ := (req.Put).Marshal()
		return Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: Op{Type: EntryKVPut, Key: req.Put.Key, Value: req.Put.Value, Data: command.Data}}
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

func PutRequestInterpreter(req *pb.PutRequest, data []byte) Op {
	// data, err := r.Marshal()
	// if err != nil {
	// 	log.Fatalf("put request interpreter marshal failed: %+v\n", req)
	// }
	return Op{Type: EntryKVPut, Key: req.Key, Value: req.Value, Data: data}
}

func ClientRequestInterpreter(req interface{}) Op {
	switch req.(type) {
	case *pb.RangeRequest:
		return Op{Type: EntryKVRO, Key: req.(*pb.RangeRequest).Key}
	// case *pb.PutRequest:
	// 	return PutRequestInterpreter(req.(*pb.PutRequest))
	default:
		req := pb.InternalRaftRequest(req.(pb.InternalRaftRequest))
		data, _ := req.Marshal()
		p := req.Put
		if p != nil {
			return PutRequestInterpreter(p, data)
		}
		return Op{Type: EntryNormal, Data: data}
	}
}

func ClientResponseInterpreter(ce ClientEntry, resp interface{}) Resp {
	// resp = resp.(pb.InternalRaftRequest)
	switch resp.(type) {
	case *pb.RangeResponse:
		return Resp{Type: ReadResp, Index: ce.Index, Key: resp.(*pb.RangeResponse).Kvs[0].Key, Value: resp.(*pb.RangeResponse).Kvs[0].Value, ctxs: ce.Ctxs}
	default:
		return Resp{Type: NormalResp, Index: ce.Index}
	}
}

func CtxInterpreter(ctx []byte) [8]byte {
	var a [8]byte
	copy(a[:8], ctx)
	return a
}

func RctxInterpreter(rctx []byte) [8]byte {
	var a [8]byte
	copy(a[:8], rctx)
	return a
}

func (s *Simulation) MsgInterpreter(msg raftpb.Message) (interface{}, uint64, uint64, Ctx) {
	switch msg.Type {
	case raftpb.MsgApp:
		return AppendEntriesArgs{From: msg.From, To: msg.To, mterm: msg.Term, mprevLogIndex: msg.Index, mprevLogTerm: msg.LogTerm, mentries: CommandsInterpreter(msg.Entries), mcommitIndex: msg.Commit}, msg.From, msg.To, nil
	case raftpb.MsgAppResp:
		return AppendEntriesReply{From: msg.From, To: msg.To, mterm: msg.Term, msuccess: !msg.Reject, mmatchIndex: msg.Index}, msg.From, msg.To, nil
	case raftpb.MsgVote:
		return RequestVoteArgs{From: msg.From, To: msg.To, mterm: msg.Term, mlastLogIndex: msg.Index, mlastLogTerm: msg.LogTerm}, msg.From, msg.To, nil
	case raftpb.MsgVoteResp:
		return RequestVoteReply{From: msg.From, To: msg.To, mterm: msg.Term, mvoteGranted: !msg.Reject}, msg.From, msg.To, nil
	case raftpb.MsgHeartbeat:
		ctx := CtxInterpreter(msg.Context)
		return AppendEntriesArgs{From: msg.From, To: msg.To, mterm: msg.Term, mprevLogIndex: s.getLastCommandIndex(), mprevLogTerm: s.getLastCommandTerm(), mentries: []Command{}, mcommitIndex: msg.Commit, ctx: ctx}, msg.From, msg.To, ctx
	case raftpb.MsgHeartbeatResp:
		ctx := CtxInterpreter(msg.Context)
		return AppendEntriesReply{From: msg.From, To: msg.To, mterm: msg.Term, msuccess: !msg.Reject, mmatchIndex: HEARTBEAT, ctx: ctx}, msg.From, msg.To, ctx
	case raftpb.MsgReadIndex:
		return ReadIndexArgs{From: msg.From, To: msg.To, rctx: RctxInterpreter(msg.Entries[0].Data)}, msg.From, msg.To, nil
	case raftpb.MsgReadIndexResp:
		return ReadIndexReply{From: msg.From, To: msg.To, mterm: msg.Term, rctx: RctxInterpreter(msg.Entries[0].Data), index: msg.Index}, msg.From, msg.To, nil
	}
	return nil, 0, 0, nil
}

//call From the upper layer
//checker function: check whether the messages are valid before actually send them out
func (s *Simulation) CheckRaftMessage(msg raftpb.Message, req interface{}) {
	valid := true
	msg_itp, _, _, ctx := s.MsgInterpreter(msg)
	s.InitializeCtx(ctx)
	switch msg.Type {
	case raftpb.MsgVote:
		valid = s.RequestVoteRequestChecker(msg_itp.(RequestVoteArgs))
	case raftpb.MsgApp:
		valid = s.AppendEntryRequestChecker(msg_itp.(AppendEntriesArgs))
	case raftpb.MsgHeartbeat:
		valid = s.AppendEntryRequestChecker(msg_itp.(AppendEntriesArgs))
	case raftpb.MsgReadIndex:
		args := msg_itp.(ReadIndexArgs)
		valid = s.ReadIndexRequestChecker(args)
		s.InitializeRctx(args.rctx)
	case raftpb.MsgVoteResp:
		valid = s.RequestVoteReplyChecker(msg_itp.(RequestVoteReply), s.HandleRaftReqMessages(req.(raftpb.Message)).(RequestVoteReply))
	case raftpb.MsgAppResp:
		valid = s.AppendEntryReplyChecker(msg_itp.(AppendEntriesReply), s.HandleRaftReqMessages(req.(raftpb.Message)).(AppendEntriesReply))
	case raftpb.MsgHeartbeatResp:
		valid = s.AppendEntryReplyChecker(msg_itp.(AppendEntriesReply), s.HandleRaftReqMessages(req.(raftpb.Message)).(AppendEntriesReply))
	case raftpb.MsgReadIndexResp:
		valid = s.ReadIndexReplyChecker(msg_itp.(ReadIndexReply))
	}
	if !valid {
		log.Fatalf("check request message error! server: %d, msg: %+v\n req: %+v\n local state: %+v\n", s.me, msg, req, s)
	}
}

func (s *Simulation) CheckClientReplyMessage(reply interface{}, ID interface{}) {
	if reply == nil {
		return
	}
	find := false
	entry := ClientEntry{}
	switch reply.(type) {
	case *pb.RangeResponse:
		find, entry = s.searchInClientBuffer(ID, EntryKVRO)
	default:
		find, entry = s.searchInClientBuffer(ID, EntryNormal)
	}

	if !find {
		log.Fatalf("%d haven't received the client req %+v, ID: %+v before\n state: %+v\n", s.me, reply, ID, s)
	}
	valid := s.ClientReplyChecker(ClientResponseInterpreter(entry, reply), ID)
	if !valid {
		log.Fatalf("check reply message error! server: %d, index: %d, reply: %+v\n local state: %+v\n", s.me, entry.Index, reply, s)
	}

	//clear the ref
	if s.State == Leader {
		for _, ctx := range entry.Ctxs {
			connection := s.Connections[ctx]
			s.Connections[ctx] = Connection{ref: connection.ref - 1, ids: connection.ids}
		}
	} else {
		for _, rctx := range entry.Ctxs {
			ri := s.ReadIndexCtx[rctx]
			s.ReadIndexCtx[rctx] = RIndex{ref: ri.ref - 1, index: ri.index}
		}
	}
}

//handler function: update the simulation state once receives the requests

func (s *Simulation) HandleClientRequest(req interface{}, ID interface{}) {
	switch req.(type) {
	case *pb.RangeRequest:
		s.ClientROBuffer = append(s.ClientROBuffer, ClientEntry{ReqID: ID, Index: s.ClientRequestHandler(ClientRequestInterpreter(req))})
	default:
		//only the leader can process other types of client request
		s.ClientOtherBuffer = append(s.ClientOtherBuffer, ClientEntry{ReqID: ID, Index: s.ClientRequestHandler(ClientRequestInterpreter(req))})
		// if s.State == Leader {
		// 	s.ClientOtherBuffer = append(s.ClientOtherBuffer, ClientEntry{Req: req, Index: s.ClientRequestHandler(ClientRequestInterpreter(req))})
		// }
	}
	s.Truncate()
	return
}

func (s *Simulation) HandleRaftReqMessages(msg raftpb.Message) interface{} {
	msg_itp, from, _, ctx := s.MsgInterpreter(msg)
	s.ConnectionAlive(from, ctx)
	switch msg.Type {
	case raftpb.MsgVote:
		r := s.RequestVoteRequestHandler(msg_itp.(RequestVoteArgs))
		return r
	case raftpb.MsgApp:
		r := s.AppendEntryRequestHandler(msg_itp.(AppendEntriesArgs))
		s.Truncate()
		return r
	case raftpb.MsgHeartbeat:
		r := s.AppendEntryRequestHandler(msg_itp.(AppendEntriesArgs))
		return r
	}
	return nil
}

func (s *Simulation) HandleRaftMessages(msg raftpb.Message) interface{} {
	msg_itp, from, _, ctx := s.MsgInterpreter(msg)
	s.ConnectionAlive(from, ctx)
	switch msg.Type {
	case raftpb.MsgProp:
		if msg.Entries[0].Type != raftpb.EntryNormal {
			log.Fatalf("the prop type is not normal\n")
		}
		command := CommandInterpreter(msg.Entries[0])
		s.ClientRequestHandler(command.Command)
		//log.printf("!!!!!!!!!!!!!!leader state after prop: %+v\n, %+v\n", msg, s)
	case raftpb.MsgVoteResp:
		s.RequestVoteReplyHandler(msg_itp.(RequestVoteReply))
	case raftpb.MsgAppResp:
		s.AppendEntryReplyHandler(msg_itp.(AppendEntriesReply))
	case raftpb.MsgHeartbeatResp:
		s.AppendEntryReplyHandler(msg_itp.(AppendEntriesReply))
	case raftpb.MsgReadIndex:
		s.ReadIndexRequestHandler(msg_itp.(ReadIndexArgs))
	case raftpb.MsgReadIndexResp:
		s.ReadIndexReplyHandler(msg_itp.(ReadIndexReply))
	}
	return nil
}

func (s *Simulation) InitializeCtx(ctx Ctx) {
	if ctx == nil {
		return
	}
	found := s.searchConnection(ctx, true, len(s.ClientROBuffer))
	if found {
		return
	}
	for i := 0; i < len(s.ClientROBuffer); i++ {
		entry := s.ClientROBuffer[i]
		s.ClientROBuffer[i] = ClientEntry{ReqID: entry.ReqID, Index: entry.Index, Ctxs: append(entry.Ctxs, ctx)}
	}
}

func (s *Simulation) InitializeRctx(rctx Ctx) {
	refs := len(s.ClientROBuffer)
	found := s.searchRctx(rctx, true, refs)
	if found {
		return
	}
	var i int
	for i = 0; i < len(s.ClientROBuffer); i++ {
		ri := s.ClientROBuffer[i]
		s.ClientROBuffer[i] = ClientEntry{ReqID: ri.ReqID, Index: ri.Index, Ctxs: append(ri.Ctxs, rctx)}
	}
	if i != refs {
		log.Fatal("the length of clientROBuffer has been changed!\n")
	}
}

func (s *Simulation) ConnectionAlive(id uint64, ctx Ctx) {
	if ctx == nil {
		return
	}
	found := s.searchConnection(ctx, false, 0)
	if found {
		connection := s.Connections[ctx]
		if inSlice(connection.ids, id) == -1 {
			s.Connections[ctx] = Connection{ref: connection.ref, ids: append(connection.ids, id)}
		}
	}
}

func (s *Simulation) RctxIndex(rctx Ctx, index uint64) {
	found := s.searchRctx(rctx, false, 0)
	if found {
		ri := s.ReadIndexCtx[rctx]
		if ri.index > index {
			s.ReadIndexCtx[rctx] = RIndex{ref: ri.ref, index: index}
		}
	}
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
	params_checker := Equal(args.mprevLogTerm, s.getCommandTerm(args.mprevLogIndex)) && s.logMatch(args.mentries)
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
		//log.printf("!!!!!!!!!requestvotereplychecker failed. exec: %+v\n sl: %+v\n", r_exec, r_sl)
	}
	return r_exec == r_sl
}

func (s *Simulation) AppendEntryReplyChecker(r_exec AppendEntriesReply, r_sl AppendEntriesReply) bool {
	// return s.searchInRaftBuffer(r_exec)
	return (r_exec.From == r_sl.From) && (r_exec.To == r_sl.To) && (r_exec.mterm == r_sl.mterm) && (r_exec.msuccess == r_sl.msuccess) && ((r_exec.mmatchIndex == r_sl.mmatchIndex) || r_exec.mmatchIndex == uint64(HEARTBEAT))
}

func (s *Simulation) ReadIndexRequestChecker(args ReadIndexArgs) bool {
	return args.From == s.me
}

func (s *Simulation) ReadIndexReplyChecker(r_exec ReadIndexReply) bool {
	find, entry := s.searchInClientBuffer(r_exec.rctx, EntryKVRO)
	if !find {
		log.Fatalf("cannot find the read only request in the buffer: %+v\n", r_exec)
	}
	return s.State == Leader && s.QuorumActive(entry.Ctxs) && r_exec.From == s.me && r_exec.mterm == s.CurrentTerm && reflect.DeepEqual(r_exec.rctx, entry.ReqID) && r_exec.index >= entry.Index
}

func (s *Simulation) ClientReplyChecker(resp Resp, ID interface{}) bool {
	switch resp.Type {
	case ReadResp:
		if s.State == Leader {
			return s.QuorumActive(resp.ctxs) && s.searchGet(resp)
		} else {
			index := s.ValidIndex(resp.ctxs)
			return s.CommitIndex >= index && s.searchGet(Resp{Type: resp.Type, Index: index, Key: resp.Key, Value: resp.Value})
		}

	case NormalResp:
		// if s.State == Leader {
		// 	return s.CommitIndex >= resp.Index
		// }

		index := uint64(math.MaxUint64)
		// data, _ := ((resp.Req).(*pb.PutRequest)).Marshal()
		//log.printf("!!!!!!!!!!!!!!!!follower checks client reply, bytes req: %+v\n", data)
		for i := resp.Index; i <= s.getLastCommandIndex(); i++ {
			data := s.getCommand(i).Command.Data
			req := new(pb.InternalRaftRequest)
			req.Unmarshal(data)
			if reflect.DeepEqual(req.Header.ID, ID) {
				index = i
				break
			}
		}
		return s.CommitIndex >= index
	default:
		return true
	}
}

func (s *Simulation) searchGet(r Resp) bool {
	if r.Index < s.getFirstCommandIndex() {
		return true
	}

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

func (s *Simulation) QuorumActive(ctxs []Ctx) bool {
	connected_peers := []uint64{}
	for i := 0; i < len(ctxs); i++ {
		ctx := ctxs[i]
		connection := s.Connections[ctx]
		for _, id := range connection.ids {
			if inSlice(connected_peers, id) == -1 {
				connected_peers = append(connected_peers, id)
			}
		}
	}
	return (len(connected_peers) + 1) > len(s.peers)/2
}

func (s *Simulation) ValidIndex(rctxs []Ctx) uint64 {
	index := uint64(math.MaxUint64)
	for i := 0; i < len(rctxs); i++ {
		rctx := rctxs[i]
		ri := s.ReadIndexCtx[rctx]
		if ri.index < index {
			index = ri.index
		}
	}
	return index
}

//Handler:
func (s *Simulation) ClientRequestHandler(args Op) uint64 {
	if s.State == Leader {
		if args.Type == EntryKVRO {
			return s.CommitIndex
		} else {
			index := s.getLastCommandIndex() + 1
			command := Command{Command: args, CommandIndex: index, CommandTerm: s.CurrentTerm}
			s.Log = append(s.Log, command)
			s.mmatchIndex[inSlice(s.peers, s.me)] = s.getLastCommandIndex()
			return index
		}
	} else {
		if args.Type == EntryKVRO {
			return math.MaxUint64
		} else {
			return s.getLastCommandIndex()
		}
	}
}

func (s *Simulation) RequestVoteRequestHandler(args RequestVoteArgs) RequestVoteReply {
	r := RequestVoteReply{From: s.me, To: args.From}
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
	return r
}

func (s *Simulation) AppendEntryRequestHandler(args AppendEntriesArgs) AppendEntriesReply {
	r := AppendEntriesReply{From: s.me, To: args.From}
	if s.DropStaleResponse(args.mterm) {
		r.mterm = s.CurrentTerm
		r.msuccess = false
		return r
	}
	s.UpdateTerm(args.mterm)
	logOK := (args.mprevLogIndex == 0) || (args.mprevLogIndex > 0 && args.mprevLogIndex <= s.getLastCommandIndex() && Equal(args.mprevLogTerm, s.getCommandTerm(args.mprevLogIndex)))

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

func (s *Simulation) ReadIndexRequestHandler(args ReadIndexArgs) {
	if args.From == 0 {
		// we have already handled the RO request sent to the leader
		return
	}
	if s.State == Leader {
		s.ClientROBuffer = append(s.ClientROBuffer, ClientEntry{ReqID: args.rctx, Index: s.ClientRequestHandler(Op{Type: EntryKVRO, Key: nil})})
	}
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
	if s.DropStaleResponse(reply.mterm) {
		return
	}
	s.UpdateTerm(reply.mterm)
	if reply.msuccess {
		if reply.mmatchIndex != HEARTBEAT {
			s.mmatchIndex[inSlice(s.peers, reply.From)] = max(reply.mmatchIndex, s.mmatchIndex[inSlice(s.peers, reply.From)])
			s.updateCommitIndex()
		}
	}
	return
}

func (s *Simulation) ReadIndexReplyHandler(reply ReadIndexReply) {
	if s.DropStaleResponse(reply.mterm) {
		return
	}
	s.UpdateTerm(reply.mterm)
	s.RctxIndex(reply.rctx, reply.index)
}

//other functions
func (s *Simulation) Truncate() {
	s.LogGC()
	s.ConnectionGC()
	s.ReadIndexGC()
}

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
}

func (s *Simulation) updateCommitIndex() {
	majority := findMajority(s.mmatchIndex)
	if majority > s.CommitIndex {
		command := s.getCommand(majority)
		if command.CommandTerm == s.CurrentTerm {
			s.CommitIndex = max(command.CommandIndex, s.CommitIndex)
		}
	}
}

func (s *Simulation) searchInClientBuffer(ID interface{}, t EntryType) (bool, ClientEntry) {
	if t == EntryKVRO {
		length := len(s.ClientROBuffer)
		for i := 0; i < length; i++ {
			if reflect.DeepEqual(s.ClientROBuffer[i].ReqID, ID) {
				entry := s.ClientROBuffer[i]
				s.ClientROBuffer = append(s.ClientROBuffer[:i], s.ClientROBuffer[i+1:]...)
				return true, entry
			}
		}
	} else {
		length := len(s.ClientOtherBuffer)
		for i := 0; i < length; i++ {
			if reflect.DeepEqual(s.ClientOtherBuffer[i].ReqID, ID) {
				entry := s.ClientOtherBuffer[i]
				s.ClientOtherBuffer = append(s.ClientOtherBuffer[:i], s.ClientOtherBuffer[i+1:]...)
				return true, entry
			}
		}
	}
	return false, ClientEntry{}
}

func (s *Simulation) searchConnection(ctx Ctx, add bool, ref int) bool {
	_, exists := s.Connections[ctx]
	if exists {
		return true
	}
	if add {
		s.Connections[ctx] = Connection{ref: ref}
	}
	return false
}

func (s *Simulation) searchRctx(rctx Ctx, add bool, ref int) bool {
	_, exists := s.ReadIndexCtx[rctx]
	if exists {
		return true
	}
	if add {
		s.ReadIndexCtx[rctx] = RIndex{ref: ref, index: math.MaxUint64}
	}
	return false
}

func (s *Simulation) LogGC() {
	if len(s.Log) <= 100 {
		return
	}
	local_index := s.getCurrentIndex(s.CommitIndex)
	if local_index == TRUNCATED {
		log.Fatal("The last committed entry has been truncated, incorrect!\n")
	}
	if local_index == NONEXIST {
		log.Fatal("The commit index is larger than the end of the log!\n")
	}
	s.Log = s.Log[local_index:]
}

func (s *Simulation) ConnectionGC() {
	if len(s.Connections) < 100 {
		return
	}

	for ctx, connection := range s.Connections {
		if connection.ref == 0 {
			delete(s.Connections, ctx)
		}
		if connection.ref < 0 {
			log.Fatalf("connection ref < 0! ERROR! ctx: %+v\n", ctx)
		}
	}
}

func (s *Simulation) ReadIndexGC() {
	if len(s.ReadIndexCtx) < 100 {
		return
	}

	for rctx, ri := range s.ReadIndexCtx {
		if ri.ref == 0 {
			delete(s.ReadIndexCtx, rctx)
		}
		if ri.ref < 0 {
			log.Fatalf("readindex ref < 0! ERROR! rctx: %+v\n, rctx", rctx)
		}
	}
}

func (s *Simulation) RctxGC() {
	if len(s.ReadIndexCtx) < 100 {
		return
	}

	for rctx, ri := range s.ReadIndexCtx {
		if ri.ref == 0 {
			delete(s.ReadIndexCtx, rctx)
		}
		if ri.ref < 0 {
			log.Fatalf("rctx ref < 0! ERROR! rctx: %+v\n", rctx)
		}
	}
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
	//log.printf("simulation state: %+v\n", s)
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
