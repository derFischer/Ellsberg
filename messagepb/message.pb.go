package messagepb

type MsgType int8

const (
	ClientRequest  MsgType = 1
	ClientRespSucc MsgType = 2
	ClientRespFail MsgType = 3
	RaftRequest    MsgType = 4
	RaftResp       MsgType = 5
)

type MessageType int64

const (
	NONE MessageType = 0
	//raft messages
	MsgApp      MessageType = 1
	MsgAppResp  MessageType = 2
	MsgVote     MessageType = 3
	MsgVoteResp MessageType = 4
	// MsgReadIndex     MessageType = 5
	// MsgReadIndexResp MessageType = 6

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
