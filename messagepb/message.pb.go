package messagepb

import (
	"log"

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
	ReqID      interface{}
	Key        []byte
	Value      []byte
	ConfChange []statepb.ReconfigChange
	Data       []byte
}

type Ctx interface{}

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

func ReconfigChangeInterpreter(rc raftpb.ConfChange) statepb.ReconfigChange {
	if rc.Type == raftpb.ConfChangeAddNode {
		return statepb.ReconfigChange{Type: statepb.AddNode, Node: rc.NodeID}
	} else {
		return statepb.ReconfigChange{Type: statepb.RemoveNode, Node: rc.NodeID}
	}
}

func ReconfigChangeEntriesInterpreter(rc raftpb.ConfChangeV2) []statepb.ReconfigChange {
	result := []statepb.ReconfigChange{}
	for _, e := range rc.Changes {
		if e.Type == raftpb.ConfChangeAddNode {
			result = append(result, statepb.ReconfigChange{Type: statepb.AddNode, Node: e.NodeID})
		}
		if e.Type == raftpb.ConfChangeRemoveNode {
			result = append(result, statepb.ReconfigChange{Type: statepb.RemoveNode, Node: e.NodeID})
		}
	}
	return result
}

func CommandInterpreter(command raftpb.Entry) statepb.Command {
	if command.Data == nil {
		if command.Type == raftpb.EntryConfChange || command.Type == raftpb.EntryConfChangeV2 {
			return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryConfChange, ConfChange: []statepb.ReconfigChange{}, Data: command.Data}}
		} else {
			return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryNormal, Data: command.Data}}
		}
	}

	if command.Type == raftpb.EntryConfChange {
		req := new(raftpb.ConfChange)
		err := req.Unmarshal(command.Data)
		if err != nil {
			log.Fatalf("fail to interpret the command data: %+v\n", command)
		}
		return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryConfChange, ConfChange: []statepb.ReconfigChange{ReconfigChangeInterpreter(*req)}, Data: command.Data}}
	}

	if command.Type == raftpb.EntryConfChangeV2 {
		req := new(raftpb.ConfChangeV2)
		err := req.Unmarshal(command.Data)
		if err != nil {
			log.Fatalf("fail to interpret the command data: %+v\n", command)
		}
		return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryConfChange, ConfChange: ReconfigChangeEntriesInterpreter(*req), Data: command.Data}}
	}

	req := new(pb.InternalRaftRequest)
	err := req.Unmarshal(command.Data)
	if err != nil {
		return statepb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: statepb.Op{Type: statepb.EntryKVPut, Data: command.Data}}
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
			if msg.Reject {
				return Message{Type: MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: false, MatchIndex: 0, Index: req.Index, IndexTerm: req.LogTerm, MEntries: CommandsInterpreter(req.Entries), CommitIndex: req.Commit, ReqTerm: req.Term}, nil
			} else {
				return Message{Type: MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: true, MatchIndex: msg.Index, Index: req.Index, IndexTerm: req.LogTerm, MEntries: CommandsInterpreter(req.Entries), CommitIndex: req.Commit, ReqTerm: req.Term}, nil
			}
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
		return Message{Type: MsgApp, From: msg.From, To: msg.To, Mterm: msg.Term, Index: statepb.Unknown, IndexTerm: statepb.Unknown, MEntries: []statepb.Command{}, CommitIndex: msg.Commit}, ctx
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

func PutRequestInterpreter(req *pb.PutRequest, data []byte, ID interface{}) Message {
	return Message{Type: ClientKVPut, Key: req.Key, Value: req.Value, Data: data, ReqID: ID}
}

func ClientRequestInterpreter(req interface{}, ID interface{}) Message {
	switch req.(type) {
	case *pb.RangeRequest:
		return Message{Type: ClientKVRO, Key: req.(*pb.RangeRequest).Key, ReqID: ID}
	case *raftpb.ConfChange:
		return Message{Type: ClientConfChange, ConfChange: []statepb.ReconfigChange{ReconfigChangeInterpreter(*(req.(*raftpb.ConfChange)))}, ReqID: ID}
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
