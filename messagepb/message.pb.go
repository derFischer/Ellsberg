package messagepb

import (
	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/raft/raftpb"
	"go.etcd.io/etcd/v3/statepb"
)

type MsgType int8

const (
	ClientRequest MsgType = 1
	ClientResp    MsgType = 2
	RaftRequest   MsgType = 3
	RaftResp      MsgType = 4
)

// type EntryType int32

// const (
// 	EntryNormal     EntryType = 0
// 	EntryKVPut      EntryType = 1
// 	EntryKVRO       EntryType = 2
// 	EntryConfChange EntryType = 3
// )

// type Command struct {
// 	CommandIndex uint64
// 	Command      Op
// 	CommandTerm  uint64
// }

// type Op struct {
// 	ReqID interface{}
// 	Type  EntryType
// 	Key   []byte
// 	Value []byte
// 	Data  []byte
// }

type MessageType int8

const (
	//raft messages
	MsgApp           MessageType = 1
	MsgAppResp       MessageType = 2
	MsgVote          MessageType = 3
	MsgVoteResp      MessageType = 4
	MsgReadIndex     MessageType = 5
	MsgReadIndexResp MessageType = 6

	//client messages
	ClientNormal     MessageType = 7
	ClientKVPut      MessageType = 8
	ClientKVRO       MessageType = 9
	ClientConfChange MessageType = 10

	ClientNormalResp MessageType = 11
	ClientROResp     MessageType = 12

	//nil message
	CheckLeader      MessageType = 13
	CheckCommitIndex MessageType = 14
	Timeout          MessageType = 15
)

type Message struct {
	Type MessageType

	//raft message
	From        uint64
	To          uint64
	Mterm       uint64
	Index       uint64
	IndexTerm   uint64
	Success     bool
	MEntries    []statepb.Command
	CommitIndex uint64
	MatchIndex  uint64
	ReqTerm     uint64

	//client messages
	ReqID interface{}
	Key   []byte
	Value []byte
	Data  []byte
}

type Ctx interface{}

// type RequestVoteArgs struct {
// 	From          uint64
// 	To            uint64
// 	MTerm         uint64
// 	MLastLogIndex uint64
// 	MLastLogTerm  uint64
// }

// type RequestVoteReply struct {
// 	From         uint64
// 	To           uint64
// 	MTerm        uint64
// 	MVoteGranted bool
// }

// type AppendEntriesArgs struct {
// 	From          uint64
// 	To            uint64
// 	MTerm         uint64
// 	MPrevLogIndex uint64
// 	MPrevLogTerm  uint64
// 	MEntries      []statepb.Command
// 	MCommitIndex  uint64
// }

// type AppendEntriesReply struct {
// 	From        uint64
// 	To          uint64
// 	MTerm       uint64
// 	MSuccess    bool
// 	MMatchIndex uint64
// }

type RespType int32

const (
	ReadResp   RespType = 0
	NormalResp RespType = 1
	ConfChange RespType = 2
)

type Resp struct {
	Type  RespType
	Key   []byte
	Value []byte
	ID    interface{}
}

func CommandInterpreter(command raftpb.Entry) statepb.Command {
	if command.Data == nil {
		return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryNormal, Data: command.Data}}
	}

	req := new(pb.InternalRaftRequest)
	err := req.Unmarshal(command.Data)
	if err != nil {
		return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryNormal, Data: command.Data}}
	}
	if req.Put != nil {
		return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryKVPut, Key: req.Put.Key, Value: req.Put.Value, Data: command.Data}}
	}
	return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryNormal, Data: command.Data}}
}

func CommandsInterpreter(commands []raftpb.Entry) []statepb.Command {
	var result []statepb.Command
	for _, command := range commands {
		result = append(result, CommandInterpreter(command))
	}
	return result
}

func CtxInterpreter(ctx []byte) interface{} {
	if len(ctx) == 0 {
		return nil
	}
	var a [8]byte
	copy(a[:8], ctx)
	return a
}

func RaftMsgInterpreter(message RaftMsg, send bool) (Message, Ctx) {
	// fmt.Printf("interprete raft message: %+v\n", message)
	msg := message.Msg.(raftpb.Message)
	switch msg.Type {
	case raftpb.MsgApp:
		return Message{Type: MsgApp, From: msg.From, To: msg.To, Mterm: msg.Term, Index: msg.Index, IndexTerm: msg.LogTerm, MEntries: CommandsInterpreter(msg.Entries), CommitIndex: msg.Commit}, nil
	case raftpb.MsgAppResp:
		if send {
			req := message.Req.(raftpb.Message)
			return Message{Type: MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, MatchIndex: msg.Index, Index: req.Index, IndexTerm: req.LogTerm, MEntries: CommandsInterpreter(req.Entries), CommitIndex: req.Commit, ReqTerm: req.Term}, nil
		} else {
			return Message{Type: MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, MatchIndex: msg.Index}, nil
		}
	case raftpb.MsgVote:
		return Message{Type: MsgVote, From: msg.From, To: msg.To, Mterm: msg.Term, Index: msg.Index, IndexTerm: msg.LogTerm}, nil
	case raftpb.MsgVoteResp:
		if send {
			req := message.Req.(raftpb.Message)
			return Message{Type: MsgVoteResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, Index: req.Index, IndexTerm: req.LogTerm, ReqTerm: req.Term}, nil
		} else {
			return Message{Type: MsgVoteResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject}, nil
		}
	case raftpb.MsgHeartbeat:
		ctx := CtxInterpreter(msg.Context)
		return Message{Type: MsgApp, From: msg.From, To: msg.To, Mterm: msg.Term, Index: 0, IndexTerm: 0, MEntries: []statepb.Command{}, CommitIndex: msg.Commit}, ctx
	case raftpb.MsgHeartbeatResp:
		// fmt.Printf("receive heartbeat message!!!\n")
		if send {
			ctx := CtxInterpreter(msg.Context)
			req := message.Req.(raftpb.Message)
			return Message{Type: MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, MatchIndex: 0, Index: 0, IndexTerm: 0, MEntries: []statepb.Command{}, CommitIndex: req.Commit, ReqTerm: req.Term}, ctx
		} else {
			ctx := CtxInterpreter(msg.Context)
			return Message{Type: MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, MatchIndex: 0}, ctx
		}
	case raftpb.MsgReadIndex:
		rctx := CtxInterpreter(msg.Entries[0].Data)
		return Message{Type: MsgReadIndex, From: msg.From, To: msg.To, ReqID: rctx}, rctx
	case raftpb.MsgReadIndexResp:
		rctx := CtxInterpreter(msg.Entries[0].Data)
		return Message{Type: MsgReadIndexResp, From: msg.From, To: msg.To, Mterm: msg.Term, ReqID: rctx, Index: msg.Index}, rctx
	}
	return Message{}, nil
}

// func RaftMsgInterpreter(msg raftpb.Message) (MsgType, interface{}, uint64, uint64, interface{}) {
// 	switch msg.Type {
// 	case raftpb.MsgApp:
// 		return MsgApp, AppendEntriesArgs{From: msg.From, To: msg.To, MTerm: msg.Term, MPrevLogIndex: msg.Index, MPrevLogTerm: msg.LogTerm, MEntries: CommandsInterpreter(msg.Entries), MCommitIndex: msg.Commit}, msg.From, msg.To, nil
// 	case raftpb.MsgAppResp:
// 		return MsgAppResp, AppendEntriesReply{From: msg.From, To: msg.To, MTerm: msg.Term, MSuccess: !msg.Reject, MMatchIndex: msg.Index}, msg.From, msg.To, nil
// 	case raftpb.MsgVote:
// 		return MsgVote, RequestVoteArgs{From: msg.From, To: msg.To, MTerm: msg.Term, MLastLogIndex: msg.Index, MLastLogTerm: msg.LogTerm}, msg.From, msg.To, nil
// 	case raftpb.MsgVoteResp:
// 		return MsgVoteResp, RequestVoteReply{From: msg.From, To: msg.To, MTerm: msg.Term, MVoteGranted: !msg.Reject}, msg.From, msg.To, nil
// 	case raftpb.MsgHeartbeat:
// 		ctx := CtxInterpreter(msg.Context)
// 		return MsgApp, AppendEntriesArgs{From: msg.From, To: msg.To, MTerm: msg.Term, MPrevLogIndex: 0, MPrevLogTerm: 0, MEntries: []statepb.Command{}, MCommitIndex: msg.Commit}, msg.From, msg.To, ctx
// 	case raftpb.MsgHeartbeatResp:
// 		ctx := CtxInterpreter(msg.Context)
// 		return MsgAppResp, AppendEntriesReply{From: msg.From, To: msg.To, MTerm: msg.Term, MSuccess: !msg.Reject, MMatchIndex: 0}, msg.From, msg.To, ctx
// 	}
// 	return 0, nil, 0, 0, nil
// }
func PutRequestInterpreter(req *pb.PutRequest, data []byte, ID interface{}) Message {
	return Message{Type: ClientKVPut, Key: req.Key, Value: req.Value, Data: data, ReqID: ID}
}

func ClientRequestInterpreter(req interface{}, ID interface{}) Message {
	switch req.(type) {
	case *pb.RangeRequest:
		return Message{Type: ClientKVRO, Key: req.(*pb.RangeRequest).Key, ReqID: ID}
	default:
		req := pb.InternalRaftRequest(req.(pb.InternalRaftRequest))
		data, _ := req.Marshal()
		p := req.Put
		if p != nil {
			return PutRequestInterpreter(p, data, ID)
		}
		return Message{Type: ClientNormal, Data: data, ReqID: ID}
	}
}

// func PutRequestInterpreter(req *pb.PutRequest, data []byte, ID interface{}) statepb.Op {
// 	return statepb.Op{Type: statepb.EntryKVPut, Key: req.Key, Value: req.Value, Data: data, ReqID: ID}
// }

// func ClientRequestInterpreter(req interface{}, ID interface{}) statepb.Op {
// 	switch req.(type) {
// 	case *pb.RangeRequest:
// 		return statepb.Op{Type: statepb.EntryKVRO, Key: req.(*pb.RangeRequest).Key, ReqID: ID}
// 	default:
// 		req := pb.InternalRaftRequest(req.(pb.InternalRaftRequest))
// 		data, _ := req.Marshal()
// 		p := req.Put
// 		if p != nil {
// 			return PutRequestInterpreter(p, data, ID)
// 		}
// 		return statepb.Op{Type: statepb.EntryNormal, Data: data, ReqID: ID}
// 	}
// }

func ClientResponseInterpreter(ID interface{}, resp interface{}) Message {
	// resp = resp.(pb.InternalRaftRequest)
	switch resp.(type) {
	case *pb.RangeResponse:
		return Message{Type: ClientROResp, ReqID: ID, Key: resp.(*pb.RangeResponse).Kvs[0].Key, Value: resp.(*pb.RangeResponse).Kvs[0].Value}
	default:
		return Message{Type: ClientNormalResp, ReqID: ID}
	}
}

// func ClientResponseInterpreter(ID interface{}, resp interface{}) Resp {
// 	// resp = resp.(pb.InternalRaftRequest)
// 	switch resp.(type) {
// 	case *pb.RangeResponse:
// 		return Resp{Type: ReadResp, ID: ID, Key: resp.(*pb.RangeResponse).Kvs[0].Key, Value: resp.(*pb.RangeResponse).Kvs[0].Value}
// 	default:
// 		return Resp{Type: NormalResp, ID: ID}
// 	}
// }

func MsgInterpreter(msg Msg) (Message, Ctx) {
	if msg.Send {
		if msg.Type == ClientResp {
			return ClientResponseInterpreter((msg.Msg).(ClientReply).ID, (msg.Msg).(ClientReply).Reply), nil
		} else {
			return RaftMsgInterpreter((msg.Msg).(RaftMsg), true)
		}
	} else {
		if msg.Type == ClientRequest {
			return ClientRequestInterpreter((msg.Msg).(ClientReq).Req, (msg.Msg).(ClientReq).ID), nil
		} else {
			return RaftMsgInterpreter((msg.Msg).(RaftMsg), false)
		}
	}
}

// func GetCtx(msg Msg) (uint64, uint64, interface{}) {
// 	if msg.Send {
// 		if msg.Type == RaftRequest || msg.Type == RaftResp {
// 			raftm := (msg.Msg).(RaftMsg)
// 			_, _, from, to, ctx := RaftMsgInterpreter(raftm.Msg.(raftpb.Message))
// 			// if raftm.Req != nil {
// 			// 	_, req := RaftMsgInterpreter(raftm.Req.(raftpb.Message))
// 			// 	return t, m, req
// 			// }
// 			return from, to, ctx
// 		} else {
// 			return 0, 0, nil
// 		}
// 	} else {
// 		if msg.Type == ClientRequest {
// 			return 0, 0, nil
// 		} else {
// 			raftm := (msg.Msg).(RaftMsg)
// 			_, _, from, to, ctx := RaftMsgInterpreter(raftm.Msg.(raftpb.Message))
// 			return from, to, ctx
// 		}
// 	}
// }

type ClientReply struct {
	Reply interface{}
	ID    interface{}
}

type ClientReq struct {
	Req interface{}
	ID  interface{}
}

type RaftMsg struct {
	Msg interface{}
	Req interface{}
}

type Msg struct {
	Send bool
	Type MsgType
	// Req  interface{}
	Msg interface{}
}

func Sending(mch chan Msg, msg Msg) {
	mch <- msg
}
