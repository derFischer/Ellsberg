package simulationpb

type MsgType int32

const (
	ClientMsg MsgType = 1
	RaftMsg   MsgType = 2
)

type Msg struct {
	Send bool
	Type MsgType
	Req  interface{}
	Msg  interface{}
}

func Sending(mch chan Msg, msg Msg) {
	mch <- msg
}
