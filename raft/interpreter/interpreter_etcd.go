//go:build etcd

package interpreter

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/gogo/protobuf/proto"
	pb "go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/v3/raft/raftpb"
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func ReconfigChangeInterpreter(rc raftpb.ConfChange) mpb.ReconfigChange {
	if rc.Type == raftpb.ConfChangeAddNode {
		return mpb.ReconfigChange{Type: mpb.AddNode, Node: rc.NodeID}
	} else {
		return mpb.ReconfigChange{Type: mpb.RemoveNode, Node: rc.NodeID}
	}
}

func ReconfigChangeEntriesInterpreter(rc raftpb.ConfChangeV2) []mpb.ReconfigChange {
	result := []mpb.ReconfigChange{}
	for _, e := range rc.Changes {
		if e.Type == raftpb.ConfChangeAddNode {
			result = append(result, mpb.ReconfigChange{Type: mpb.AddNode, Node: e.NodeID})
		}
		if e.Type == raftpb.ConfChangeRemoveNode {
			result = append(result, mpb.ReconfigChange{Type: mpb.RemoveNode, Node: e.NodeID})
		}
	}
	return result
}

func CommandInterpreter(command raftpb.Entry) mpb.Command {
	if command.Data == nil {
		if command.Type == raftpb.EntryConfChange || command.Type == raftpb.EntryConfChangeV2 {
			return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryConfChange, ConfChange: []mpb.ReconfigChange{}}}
		} else {
			return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryEmpty}}
		}
	}

	if command.Type == raftpb.EntryConfChange {
		req := new(raftpb.ConfChange)
		err := req.Unmarshal(command.Data)
		if err != nil {
			log.Fatalf("fail to interpret the command data: %+v\n", command)
		}
		return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryConfChange, ConfChange: []mpb.ReconfigChange{ReconfigChangeInterpreter(*req)}, ReqID: req.ID}}
	}

	if command.Type == raftpb.EntryConfChangeV2 {
		req := new(raftpb.ConfChangeV2)
		err := req.Unmarshal(command.Data)
		if err != nil {
			log.Fatalf("fail to interpret the command data: %+v\n", command)
		}
		return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryConfChange, ConfChange: ReconfigChangeEntriesInterpreter(*req), ReqID: req.Context}}
	}

	req := new(pb.InternalRaftRequest)
	err := req.Unmarshal(command.Data)
	var req_id interface{}
	if req.Header != nil {
		req_id = req.Header.ID
	}
	if err != nil {
		return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryNormal, ReqID: req_id}}
	}
	if req.Put != nil {
		return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryKVPut, ReqID: req_id}}
	}
	return mpb.Command{CommandIndex: command.Index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryNormal, ReqID: req_id}}
}

func CommandsInterpreter(commands []raftpb.Entry) []mpb.Command {
	var result []mpb.Command
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

func RaftMsgInterpreter(message RaftMsg, send bool) mpb.Message {
	// fmt.Printf("interprete raft message: %+v\n", message)
	msg := message.Msg.(raftpb.Message)
	switch msg.Type {
	case raftpb.MsgApp:
		ctx := CtxInterpreter(msg.Context)
		return mpb.Message{Type: mpb.MsgApp, From: msg.From, To: msg.To, Mterm: msg.Term, Index: msg.Index, IndexTerm: msg.LogTerm, MEntries: CommandsInterpreter(msg.Entries), CommitIndex: msg.Commit, Ctx: ctx}
	case raftpb.MsgAppResp:
		ctx := CtxInterpreter(msg.Context)
		if send {
			req := message.Req.(raftpb.Message)
			if msg.Reject {
				return mpb.Message{Type: mpb.MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: false, MatchIndex: 0, Index: req.Index, IndexTerm: req.LogTerm, MEntries: CommandsInterpreter(req.Entries), CommitIndex: req.Commit, ReqTerm: req.Term, Ctx: ctx}
			} else {
				return mpb.Message{Type: mpb.MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: true, MatchIndex: msg.Index, Index: req.Index, IndexTerm: req.LogTerm, MEntries: CommandsInterpreter(req.Entries), CommitIndex: req.Commit, ReqTerm: req.Term, Ctx: ctx}
			}
		} else {
			return mpb.Message{Type: mpb.MsgAppResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, MatchIndex: msg.Index, Ctx: ctx}
		}
	case raftpb.MsgVote:
		return mpb.Message{Type: mpb.MsgVote, From: msg.From, To: msg.To, Mterm: msg.Term, Index: msg.Index, IndexTerm: msg.LogTerm}
	case raftpb.MsgVoteResp:
		if send {
			req := message.Req.(raftpb.Message)
			return mpb.Message{Type: mpb.MsgVoteResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject, Index: req.Index, IndexTerm: req.LogTerm, ReqTerm: req.Term}
		} else {
			return mpb.Message{Type: mpb.MsgVoteResp, From: msg.From, To: msg.To, Mterm: msg.Term, Success: !msg.Reject}
		}
	default:
		panic(fmt.Sprintf("unknown raft message type: %+v\n", msg.Type))
	}
}

func PutRequestInterpreter(req *pb.PutRequest, data []byte, ID interface{}) mpb.Message {
	return mpb.Message{Type: mpb.ClientKVPut, ReqID: ID}
}

func ClientRequestInterpreter(req interface{}, ID interface{}) mpb.Message {
	switch r := req.(type) {
	case pb.RangeRequest:
		return mpb.Message{Type: mpb.ClientKVRO, ReqID: ID}
	case raftpb.ConfChange:
		return mpb.Message{Type: mpb.ClientConfChange, ConfChange: []mpb.ReconfigChange{ReconfigChangeInterpreter(r)}, ReqID: ID}
	default:
		req := pb.InternalRaftRequest(req.(pb.InternalRaftRequest))
		data, _ := req.Marshal()
		p := req.Put
		if p != nil {
			return PutRequestInterpreter(p, data, ID)
		}
		return mpb.Message{Type: mpb.ClientNormal, ReqID: ID}
	}
}

func ClientResponseInterpreter(ID interface{}, resp interface{}) mpb.Message {
	// resp = resp.(pb.InternalRaftRequest)
	switch r := resp.(type) {
	case pb.RangeResponse:
		// A failed read is reported without a response body, which decodes to
		// a response without a header. A successful read always has a header,
		// even when it finds no keys, and is checked like any other read.
		if r.Header == nil {
			return mpb.Message{Type: mpb.ClientROResp, ReqID: ID, Success: false}
		}
		return mpb.Message{Type: mpb.ClientROResp, ReqID: ID, Success: true}
	default:
		return mpb.Message{Type: mpb.ClientNormalResp, ReqID: ID, Success: true}
	}
}

func MsgInterpreter(msg Msg) mpb.Message {
	if msg.Send {
		if msg.Type == mpb.ClientRespSucc {
			return ClientResponseInterpreter((msg.Msg).(ClientReply).ID, (msg.Msg).(ClientReply).Reply)
		} else if msg.Type == mpb.ClientRespFail {
			return mpb.Message{Type: mpb.ClientNormalResp, ReqID: (msg.Msg).(ClientReply).ID, Success: false}
		} else {
			return RaftMsgInterpreter((msg.Msg).(RaftMsg), true)
		}
	} else {
		if msg.Type == mpb.ClientRequest {
			return ClientRequestInterpreter((msg.Msg).(ClientReq).Req, (msg.Msg).(ClientReq).ID)
		} else {
			return RaftMsgInterpreter((msg.Msg).(RaftMsg), false)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Marshal & Unmarshal
//////////////////////////////////////////////////////////////////////////////////////////////////

type PbMessageType int8

const (
	RangeResponse         PbMessageType = 0
	PutResponse           PbMessageType = 1
	DeleteRangeResponse   PbMessageType = 2
	TxnResponse           PbMessageType = 3
	MemberAddResponse     PbMessageType = 15
	MemberRemoveResponse  PbMessageType = 16
	MemberUpdateResponse  PbMessageType = 17
	MemberListResponse    PbMessageType = 18
	MemberPromoteResponse PbMessageType = 19
)

func MarshalRaftPbMessage(msg *raftpb.Message) []byte {
	bytes, err := proto.Marshal(msg)
	PanicIfMarshalError(err)
	return bytes
}

func UnmarshalRaftPbMessage(bytes []byte) *raftpb.Message {
	pbMsg := &raftpb.Message{}
	err := pbMsg.Unmarshal(bytes)
	PanicIfUnmarshalError(err)
	return pbMsg
}

func ClientRespType(resp proto.Message) uint64 {
	if resp == nil {
		return 42
	}
	switch resp.(type) {
	case *pb.RangeResponse:
		return 0
	case *pb.PutResponse:
		return 1
	case *pb.DeleteRangeResponse:
		return 2
	case *pb.TxnResponse:
		return 3
	case *pb.CompactionResponse:
		return 4
	case *pb.MemberAddResponse:
		return 15
	case *pb.MemberRemoveResponse:
		return 16
	case *pb.MemberUpdateResponse:
		return 17
	case *pb.MemberListResponse:
		return 18
	case *pb.MemberPromoteResponse:
		return 19
	default:
		panic(fmt.Sprintf("Unknown response type:%v", resp))
	}
}

func UnmarshalClientResp(pbType PbMessageType, data []byte) interface{} {
	switch pbType {
	case 0:
		pbMsg := pb.RangeResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 1:
		pbMsg := pb.PutResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 2:
		pbMsg := pb.DeleteRangeResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 3:
		pbMsg := pb.TxnResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 4:
		pbMsg := pb.CompactionResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 15:
		pbMsg := pb.MemberAddResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 16:
		pbMsg := pb.MemberRemoveResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 17:
		pbMsg := pb.MemberUpdateResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 18:
		pbMsg := pb.MemberListResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 19:
		pbMsg := pb.MemberPromoteResponse{}
		err := pbMsg.Unmarshal(data)
		PanicIfUnmarshalError(err)
		return pbMsg
	case 42:
		return nil
	default:
		panic("unknown pbType")
	}
}

func ParsePbMsgBytesToMsg(msg_bytes []byte) Msg {
	send := bool(binary.LittleEndian.Uint64(msg_bytes[0:8]) == 1)
	msgType := (mpb.MsgType)(binary.LittleEndian.Uint64(msg_bytes[8:16]))
	formed_msg := Msg{}
	switch msgType {
	case mpb.ClientRequest:
		pbType := binary.LittleEndian.Uint64(msg_bytes[16:24])
		reqId := binary.LittleEndian.Uint64(msg_bytes[24:32])
		reqSize := binary.LittleEndian.Uint64(msg_bytes[32:40])
		switch pbType {
		case 0:
			r := pb.InternalRaftRequest{}
			err := r.Unmarshal(msg_bytes[40 : 40+reqSize])
			PanicIfUnmarshalError(err)
			formed_msg = Msg{Send: send, Type: msgType, Msg: ClientReq{Req: r, ID: reqId}}
		case 1:
			r := pb.RangeRequest{}
			err := r.Unmarshal(msg_bytes[40 : 40+reqSize])
			PanicIfUnmarshalError(err)
			formed_msg = Msg{Send: send, Type: msgType, Msg: ClientReq{Req: r, ID: reqId}}
		case 2:
			r := raftpb.ConfChange{}
			err := r.Unmarshal(msg_bytes[40 : 40+reqSize])
			PanicIfUnmarshalError(err)
			formed_msg = Msg{Send: send, Type: msgType, Msg: ClientReq{Req: r, ID: reqId}}
		default:
			panic(fmt.Sprintf("unknown pbType: %d", pbType))
		}
	case mpb.ClientRespSucc:
		replyId := binary.LittleEndian.Uint64(msg_bytes[16:24])
		replyType := (PbMessageType)(binary.LittleEndian.Uint64(msg_bytes[24:32]))
		replySize := binary.LittleEndian.Uint64(msg_bytes[32:40])
		resp := UnmarshalClientResp(replyType, msg_bytes[40:40+replySize])
		formed_msg = Msg{Send: send, Type: msgType, Msg: ClientReply{Reply: resp, ID: replyId, Success: true}}
	case mpb.ClientRespFail:
		replyId := binary.LittleEndian.Uint64(msg_bytes[16:24])
		formed_msg = Msg{Send: send, Type: msgType, Msg: ClientReply{ID: replyId, Success: false}}
	case mpb.RaftRequest:
		msgSize := binary.LittleEndian.Uint64(msg_bytes[16:24])
		msg := UnmarshalRaftPbMessage(msg_bytes[24 : 24+msgSize])
		reqSize := binary.LittleEndian.Uint64(msg_bytes[24+msgSize : 32+msgSize])
		req := UnmarshalRaftPbMessage(msg_bytes[32+msgSize : 32+msgSize+reqSize])
		formed_msg = Msg{Send: send, Type: msgType, Msg: RaftMsg{Msg: *msg, Req: *req}}
	case mpb.RaftResp:
		msgSize := binary.LittleEndian.Uint64(msg_bytes[16:24])
		msg := UnmarshalRaftPbMessage(msg_bytes[24 : 24+msgSize])
		reqSize := binary.LittleEndian.Uint64(msg_bytes[24+msgSize : 32+msgSize])
		req := UnmarshalRaftPbMessage(msg_bytes[32+msgSize : 32+msgSize+reqSize])
		formed_msg = Msg{Send: send, Type: msgType, Msg: RaftMsg{Msg: *msg, Req: *req}}
	default:
		panic("unknown msgType")
	}

	return formed_msg
}
