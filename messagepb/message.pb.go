package messagepb

import (
	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/raft/raftpb"
	"go.etcd.io/etcd/v3/statepb"
)

type MsgType int32

const (
	ClientRequest MsgType = 1
	ClientResp    MsgType = 2
	RaftRequest   MsgType = 3
	RaftResp      MsgType = 4

	MsgApp      MsgType = 5
	MsgAppResp  MsgType = 6
	MsgVote     MsgType = 7
	MsgVoteResp MsgType = 8
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

type RequestVoteArgs struct {
	From          uint64
	To            uint64
	MTerm         uint64
	MLastLogIndex uint64
	MLastLogTerm  uint64
}

type RequestVoteReply struct {
	From         uint64
	To           uint64
	MTerm        uint64
	MVoteGranted bool
}

type AppendEntriesArgs struct {
	From          uint64
	To            uint64
	MTerm         uint64
	MPrevLogIndex uint64
	MPrevLogTerm  uint64
	MEntries      []statepb.Command
	MCommitIndex  uint64
}

type AppendEntriesReply struct {
	From        uint64
	To          uint64
	MTerm       uint64
	MSuccess    bool
	MMatchIndex uint64
}

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

func RaftMsgInterpreter(msg raftpb.Message) (MsgType, interface{}) {
	switch msg.Type {
	case raftpb.MsgApp:
		return MsgApp, AppendEntriesArgs{From: msg.From, To: msg.To, MTerm: msg.Term, MPrevLogIndex: msg.Index, MPrevLogTerm: msg.LogTerm, MEntries: CommandsInterpreter(msg.Entries), MCommitIndex: msg.Commit}
	case raftpb.MsgAppResp:
		return MsgAppResp, AppendEntriesReply{From: msg.From, To: msg.To, MTerm: msg.Term, MSuccess: !msg.Reject, MMatchIndex: msg.Index}
	case raftpb.MsgVote:
		return MsgVote, RequestVoteArgs{From: msg.From, To: msg.To, MTerm: msg.Term, MLastLogIndex: msg.Index, MLastLogTerm: msg.LogTerm}
	case raftpb.MsgVoteResp:
		return MsgVoteResp, RequestVoteReply{From: msg.From, To: msg.To, MTerm: msg.Term, MVoteGranted: !msg.Reject}
	case raftpb.MsgHeartbeat:
		return MsgApp, AppendEntriesArgs{From: msg.From, To: msg.To, MTerm: msg.Term, MPrevLogIndex: 0, MPrevLogTerm: 0, MEntries: []statepb.Command{}, MCommitIndex: msg.Commit}
	case raftpb.MsgHeartbeatResp:
		return MsgAppResp, AppendEntriesReply{From: msg.From, To: msg.To, MTerm: msg.Term, MSuccess: !msg.Reject, MMatchIndex: 0}
	}
	return 0, nil
}

func PutRequestInterpreter(req *pb.PutRequest, data []byte, ID interface{}) statepb.Op {
	return statepb.Op{Type: statepb.EntryKVPut, Key: req.Key, Value: req.Value, Data: data, ReqID: ID}
}

func ClientRequestInterpreter(req interface{}, ID interface{}) statepb.Op {
	switch req.(type) {
	case *pb.RangeRequest:
		return statepb.Op{Type: statepb.EntryKVRO, Key: req.(*pb.RangeRequest).Key, ReqID: ID}
	default:
		req := pb.InternalRaftRequest(req.(pb.InternalRaftRequest))
		data, _ := req.Marshal()
		p := req.Put
		if p != nil {
			return PutRequestInterpreter(p, data, ID)
		}
		return statepb.Op{Type: statepb.EntryNormal, Data: data, ReqID: ID}
	}
}

func ClientResponseInterpreter(ID interface{}, resp interface{}) Resp {
	// resp = resp.(pb.InternalRaftRequest)
	switch resp.(type) {
	case *pb.RangeResponse:
		return Resp{Type: ReadResp, ID: ID, Key: resp.(*pb.RangeResponse).Kvs[0].Key, Value: resp.(*pb.RangeResponse).Kvs[0].Value}
	default:
		return Resp{Type: NormalResp, ID: ID}
	}
}

func MsgInterpreter(msg Msg) (MsgType, interface{}, interface{}) {
	if msg.Send {
		if msg.Type == RaftRequest || msg.Type == RaftResp {
			raftm := (msg.Msg).(RaftMsg)
			t, m := RaftMsgInterpreter(raftm.Msg.(raftpb.Message))
			// if raftm.Req != nil {
			// 	_, req := RaftMsgInterpreter(raftm.Req.(raftpb.Message))
			// 	return t, m, req
			// }
			return t, m, raftm.Req
		} else {
			return ClientResp, ClientResponseInterpreter((msg.Msg).(ClientReply).ID, (msg.Msg).(ClientReply).Reply), nil
		}
	} else {
		if msg.Type == ClientRequest {
			return ClientRequest, ClientRequestInterpreter((msg.Msg).(ClientReq).Req, (msg.Msg).(ClientReq).ID), nil
		} else {
			raftm := (msg.Msg).(RaftMsg)
			t, m := RaftMsgInterpreter(raftm.Msg.(raftpb.Message))
			return t, m, nil
		}
	}
}

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
