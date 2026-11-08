package interpreter

import (
	"fmt"

	"go.nyu.edu/protosim/raft/messagepb"
)

// type Ctx interface{}

type RespType int32

const (
	ReadResp   RespType = 0
	NormalResp RespType = 1
	ConfChange RespType = 2
)

type Resp struct {
	Type RespType
	ID   interface{}
}

type SimMsg interface {
	ParsePbMsgBytesToMsg(msg_bytes []byte) messagepb.Message
}

type ClientReply struct {
	Success bool
	Reply   interface{}
	ID      interface{}
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
	Type messagepb.MsgType
	// Req  interface{}
	Msg interface{}
}

func PanicIfMarshalError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Marshal failed, err:%v\n", err))
	}
}

func PanicIfUnmarshalError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Unmarshal failed, err:%v\n", err))
	}
}

type InterpreterI interface {
	MsgInterpreter(msg Msg) messagepb.Message
	ParsePbMsgBytesToMsg(msg_bytes []byte) Msg
}
