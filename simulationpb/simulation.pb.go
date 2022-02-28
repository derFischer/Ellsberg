package simulationpb

type MsgType int32

const (
	ClientRequest MsgType = 1
	ClientResp    MsgType = 2
	RaftM         MsgType = 3
)

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
