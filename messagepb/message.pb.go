package messagepb

type MsgType int8

const (
	ClientRequest  MsgType = 1
	ClientRespSucc MsgType = 2
	ClientRespFail MsgType = 3
	RaftRequest    MsgType = 4
	RaftResp       MsgType = 5
)

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
