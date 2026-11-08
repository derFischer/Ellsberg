//go:build redis

package interpreter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.nyu.edu/protosim/raft/interpreter/redis/proto"
	mpb "go.nyu.edu/protosim/raft/messagepb"
)

var APP string

type RedisInterpreter struct {
}

type EntryType uint64

const (
	RAFT_LOGTYPE_NORMAL             EntryType = 0
	RAFT_LOGTYPE_ADD_NONVOTING_NODE EntryType = 1
	RAFT_LOGTYPE_ADD_NODE           EntryType = 2
	RAFT_LOGTYPE_REMOVE_NODE        EntryType = 3
	RAFT_LOGTYPE_NO_OP              EntryType = 4
)

// TODO: check whether specify index is reasonable
func CommandInterpreter(command RedisRaftEntry, index uint64) mpb.Command {
	if APP == "" {
		// APPNAME will be set in main.go
		APP = os.Getenv("APPNAME")
	}
	if command.Type == RAFT_LOGTYPE_ADD_NONVOTING_NODE {
		return mpb.Command{CommandIndex: index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryEmpty, ReqID: command.ReqID}}
	}
	if command.Data == nil || len(command.Data) == 0 {
		if command.Type == RAFT_LOGTYPE_ADD_NODE || command.Type == RAFT_LOGTYPE_REMOVE_NODE {
			return mpb.Command{CommandIndex: index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryConfChange, ConfChange: []mpb.ReconfigChange{}, ReqID: command.ReqID}}
		} else {
			return mpb.Command{CommandIndex: index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryEmpty, ReqID: command.ReqID}}
		}
	}

	if command.Type == RAFT_LOGTYPE_ADD_NODE || command.Type == RAFT_LOGTYPE_REMOVE_NODE {
		// TODO: for redis, the node_id is `int`, shall we use Uint32 here?
		nodeId32 := binary.LittleEndian.Uint32(command.Data[:4])
		nodeId64 := uint64(nodeId32)
		var confChangeType mpb.ReconfigChangeType
		if command.Type == RAFT_LOGTYPE_ADD_NODE {
			confChangeType = mpb.AddNode
		} else {
			confChangeType = mpb.RemoveNode
		}
		// TODO: Note that I used nil for Data
		// 	Because I didn't unmarshal Data for client requests, which may causes unmatch later.
		return mpb.Command{CommandIndex: index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryConfChange, ConfChange: []mpb.ReconfigChange{{Type: confChangeType, Node: nodeId64}}, ReqID: command.ReqID}}
	}

	if bytes.HasPrefix(command.Data, []byte("*1\n*3\n$3\nSET")) || bytes.HasPrefix(command.Data, []byte("*1\n*3\n$3\nset")) {
		return mpb.Command{CommandIndex: index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryKVPut, ReqID: command.ReqID}}
	} else {
		return mpb.Command{CommandIndex: index, CommandTerm: command.Term, Command: mpb.Op{Type: mpb.EntryNormal, ReqID: command.ReqID}}
	}
}

func CommandsInterpreter(commands []RedisRaftEntry, prevLogIndex uint64) []mpb.Command {
	var result []mpb.Command
	for i, command := range commands {
		result = append(result, CommandInterpreter(command, prevLogIndex+1+uint64(i)))
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
	msg := message.Msg.(RedisMessage)
	switch msg.Type {
	case "RAFT.AE":
		var ctx interface{}
		if msg.MsgId != 0 {
			ctx = msg.MsgId
		} else {
			ctx = nil
		}
		return mpb.Message{Type: mpb.MsgApp, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Index: msg.PrevLogIdx, IndexTerm: msg.PrevLogTerm, MEntries: CommandsInterpreter(msg.Entries, msg.PrevLogIdx), CommitIndex: msg.LeaderCommit, Ctx: ctx}
	case "RAFT.AER":
		var ctx interface{}
		if msg.MsgId != 0 {
			ctx = msg.MsgId
		} else {
			ctx = nil
		}
		if send {
			req := message.Req.(RedisMessage)
			if msg.Success {
				return mpb.Message{Type: mpb.MsgAppResp, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Success: msg.Success, MatchIndex: msg.CurrentIdx, Index: req.PrevLogIdx, IndexTerm: req.PrevLogTerm, MEntries: CommandsInterpreter(req.Entries, req.PrevLogIdx), CommitIndex: req.LeaderCommit, ReqTerm: req.Term, Ctx: ctx}
			} else {
				return mpb.Message{Type: mpb.MsgAppResp, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Success: msg.Success, MatchIndex: 0, Index: req.PrevLogIdx, IndexTerm: req.PrevLogTerm, MEntries: CommandsInterpreter(req.Entries, req.PrevLogIdx), CommitIndex: req.LeaderCommit, ReqTerm: req.Term, Ctx: ctx}
			}
		} else {
			return mpb.Message{Type: mpb.MsgAppResp, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Success: msg.Success, MatchIndex: msg.CurrentIdx, Ctx: ctx}
		}
	case "RAFT.REQUESTVOTE":
		if msg.SrcId != msg.CandidateId {
			panic("msg.SrcId != msg.CandidateId")
		}
		return mpb.Message{Type: mpb.MsgVote, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Index: msg.LastLogIdx, IndexTerm: msg.LastLogTerm}
	case "RAFT.REQUESTVOTEREPLY":
		if send {
			req := message.Req.(RedisMessage)
			return mpb.Message{Type: mpb.MsgVoteResp, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Success: msg.VoteGranted, Index: req.LastLogIdx, IndexTerm: req.LastLogTerm, ReqTerm: req.Term}
		} else {
			return mpb.Message{Type: mpb.MsgVoteResp, From: msg.SrcId, To: msg.DstId, Mterm: msg.Term, Success: msg.VoteGranted}
		}
	}
	return mpb.Message{}
}

func ClientRequestInterpreter(req interface{}, ID interface{}) mpb.Message {
	arr := req.(*proto.Array)
	reqType, err := arr.NextString()
	PanicIfUnmarshalError(err)
	switch reqType {
	case "RAFT":
		cmdType, err := arr.NextString()
		PanicIfUnmarshalError(err)
		cmdType = strings.ToUpper(cmdType)
		switch cmdType {
		case "SET":
			msg := mpb.Message{Type: mpb.ClientKVPut, ReqID: ID}
			return msg
		case "GET":
			msg := mpb.Message{Type: mpb.ClientKVRO, ReqID: ID}
			return msg
		default:
			panic(fmt.Sprintf("unknown raft command type: %s", cmdType))
		}
	case "RAFT.NODE":
		nodeOpType, err := arr.NextString()
		PanicIfUnmarshalError(err)
		nodeIDStr, err := arr.NextString()
		PanicIfUnmarshalError(err)
		nodeIDInt, err := strconv.Atoi(nodeIDStr)
		PanicIfUnmarshalError(err)
		nodeID := uint64(nodeIDInt)
		switch nodeOpType {
		case "ADD":
			return mpb.Message{Type: mpb.ClientConfChange, ConfChange: []mpb.ReconfigChange{{Type: mpb.AddNode, Node: nodeID}}, ReqID: ID}
		case "REMOVE":
			return mpb.Message{Type: mpb.ClientConfChange, ConfChange: []mpb.ReconfigChange{{Type: mpb.RemoveNode, Node: nodeID}}, ReqID: ID}
		default:
			panic("Unknown raft node operation type: " + nodeOpType)
		}
	default:
		panic(fmt.Sprintf("unknown request type: %s", reqType))
	}
}

func ClientResponseInterpreter(ID interface{}, resp interface{}) mpb.Message {
	arr := resp.(*proto.Array)
	respCmdType, err := arr.NextString()
	if err != proto.ErrEOM {
		PanicIfUnmarshalError(err)
		respCmdType = strings.ToUpper(respCmdType)
	}
	switch respCmdType {
	case "GET":
		return mpb.Message{Type: mpb.ClientROResp, ReqID: ID, Success: true}
	case "SET":
		return mpb.Message{Type: mpb.ClientNormalResp, ReqID: ID, Success: true}
	default:
		return mpb.Message{Type: mpb.ClientNormalResp, ReqID: ID, Success: true}
	}
}

func MsgInterpreter(msg Msg) mpb.Message {
	if msg.Send {
		if msg.Type == mpb.ClientRespSucc {
			return ClientResponseInterpreter((msg.Msg).(ClientReply).ID, (msg.Msg).(ClientReply).Reply.(*proto.Array))
		} else if msg.Type == mpb.ClientRespFail {
			return mpb.Message{Type: mpb.ClientNormalResp, ReqID: (msg.Msg).(ClientReply).ID, Success: false}
		} else {
			return RaftMsgInterpreter((msg.Msg).(RaftMsg), true)
		}
	} else {
		if msg.Type == mpb.ClientRequest {
			return ClientRequestInterpreter((msg.Msg).(ClientReq).Req.(*proto.Array), (msg.Msg).(ClientReq).ID)
		} else {
			return RaftMsgInterpreter((msg.Msg).(RaftMsg), false)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Marshal & Unmarshal
//////////////////////////////////////////////////////////////////////////////////////////////////

func Atouint64(s string) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	PanicIfUnmarshalError(err)
	return i
}

type RedisMessage struct {
	// parse first
	Type       string
	ReqId      uint64 // it is the according req id for response
	SrcId      uint64
	DstId      uint64
	MsgContent string
	arr        *proto.Array
	// parse later
	LeaderId     uint64
	Term         uint64
	PrevLogIdx   uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	MsgId        uint64
	Entries      []RedisRaftEntry
	Success      bool
	CurrentIdx   uint64
	Prevote      uint64
	CandidateId  uint64
	LastLogIdx   uint64
	LastLogTerm  uint64
	RequestTerm  uint64
	VoteGranted  bool
}

type RedisRaftEntry struct {
	Term    uint64
	Id      uint64
	Session uint64
	Type    EntryType
	Data    []byte
	ReqID   uint64
}

func UnmarshalRedisMessage(arr *proto.Array, reqId uint64) RedisMessage {
	// innerMsgType
	innerMsgTypeStr, err := arr.NextString()
	PanicIfUnmarshalError(err)
	// dstId
	dstIdStr, err := arr.NextString()
	PanicIfUnmarshalError(err)
	dstId := Atouint64(dstIdStr)
	// srcId
	srcIdStr, err := arr.NextString()
	PanicIfUnmarshalError(err)
	srcId := Atouint64(srcIdStr)
	// RedisMessage
	msg := RedisMessage{Type: innerMsgTypeStr, SrcId: srcId, DstId: dstId, arr: arr}
	switch innerMsgTypeStr {
	case "RAFT.AE":
		// msg content
		msgContentStr, err := msg.arr.NextString()
		PanicIfUnmarshalError(err)
		fmt.Sscanf(msgContentStr, "%d:%d:%d:%d:%d:%d", &msg.LeaderId, &msg.Term, &msg.PrevLogIdx, &msg.PrevLogTerm, &msg.LeaderCommit, &msg.MsgId)
		// numEntries
		numEntriesStr, err := msg.arr.NextString()
		PanicIfUnmarshalError(err)
		numEntries := Atouint64(numEntriesStr)
		for i := 0; i < int(numEntries); i++ {
			entry := RedisRaftEntry{}
			entryHdrStr, err := msg.arr.NextString()
			PanicIfUnmarshalError(err)
			fmt.Sscanf(entryHdrStr, "%d:%d:%d:%d", &entry.Term, &entry.Id, &entry.Session, &entry.Type)
			entry.Data, err = msg.arr.NextBytes()
			if bytes.HasPrefix(entry.Data, []byte("*0\n$0\n\n")) {
				// Trim \"*0\\n$0\\n\\n\" as prefix for asking and acl
				entry.Data = bytes.TrimPrefix(entry.Data, []byte("*0\n$0\n\n"))
			}
			PanicIfUnmarshalError(err)
			msg.Entries = append(msg.Entries, entry)
		}
		joined_req_ids, err := msg.arr.NextString()
		if err == nil && joined_req_ids != "" {
			for i, entryReqId := range strings.Split(joined_req_ids, ":") {
				msg.Entries[i].ReqID = Atouint64(entryReqId)
			}
		}
		msg.ReqId = reqId
	case "RAFT.AER":
		// msg content
		msgContentStr, err := msg.arr.NextString()
		PanicIfUnmarshalError(err)
		fmt.Sscanf(msgContentStr, "%d:%t:%d:%d", &msg.Term, &msg.Success, &msg.CurrentIdx, &msg.MsgId)
		msg.ReqId = reqId
	case "RAFT.REQUESTVOTE":
		msgContentStr, err := msg.arr.NextString()
		PanicIfUnmarshalError(err)
		fmt.Sscanf(msgContentStr, "%d:%d:%d:%d:%d", &msg.Prevote, &msg.Term, &msg.CandidateId, &msg.LastLogIdx, &msg.LastLogTerm)
		msg.ReqId = reqId
	case "RAFT.REQUESTVOTEREPLY":
		msgContentStr, err := msg.arr.NextString()
		PanicIfUnmarshalError(err)
		fmt.Sscanf(msgContentStr, "%d:%d:%d:%t", &msg.Prevote, &msg.RequestTerm, &msg.Term, &msg.VoteGranted)
		msg.ReqId = reqId
	default:
		panic("unknown innerMsgTypeStr: " + innerMsgTypeStr)
	}
	return msg
}

func ParsePbMsgBytesToMsg(msg_bytes []byte) Msg {
	parser := proto.NewParserWithBytes(msg_bytes)
	msg, err := parser.Next()
	if err != nil {
		fmt.Printf("error msg: %v\n", string(msg_bytes))
	}
	PanicIfUnmarshalError(err)
	arr, err := msg.Array()
	PanicIfUnmarshalError(err)

	// send
	sendStr, err := arr.NextString()
	PanicIfUnmarshalError(err)
	var send bool
	if sendStr == "1" {
		send = true
	} else {
		send = false
	}
	// msgType
	msgTypeStr, err := arr.NextString()
	PanicIfUnmarshalError(err)
	msgType := mpb.MsgType(Atouint64(msgTypeStr))
	// reqId
	reqIdStr, err := arr.NextString()
	PanicIfUnmarshalError(err)
	reqId := Atouint64(reqIdStr)

	formedMsg := Msg{}
	switch msgType {
	case mpb.ClientRequest:
		// req
		req := ClientReq{Req: arr, ID: reqId}
		formedMsg = Msg{Send: send, Type: msgType, Msg: req}
	case mpb.ClientRespSucc:
		// reply
		reply := ClientReply{Reply: arr, ID: reqId, Success: true}
		formedMsg = Msg{Send: send, Type: msgType, Msg: reply}
	case mpb.ClientRespFail:
		// reply
		reply := ClientReply{Reply: arr, ID: reqId, Success: false}
		formedMsg = Msg{Send: send, Type: msgType, Msg: reply}
	case mpb.RaftRequest:
		// msg
		msg := UnmarshalRedisMessage(arr, reqId)
		// formedMsg
		formedMsg = Msg{Send: send, Type: msgType, Msg: RaftMsg{Msg: msg, Req: nil}}
	case mpb.RaftResp:
		// msg & req
		msg := UnmarshalRedisMessage(arr, reqId)
		var req interface{}
		if send {
			req = UnmarshalRedisMessage(arr, reqId)
		} else {
			req = nil
		}
		// formedMsg
		formedMsg = Msg{Send: send, Type: msgType, Msg: RaftMsg{Msg: msg, Req: req}}
	default:
		panic(fmt.Sprintf("unknown msg type: %+v\n", msgType))
	}
	return formedMsg
}
