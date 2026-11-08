//go:build zk

package interpreter

import (
	"bufio"
	"encoding/binary"
	"fmt"

	"go.nyu.edu/protosim/zab/messagepb"
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

type SimConnMessage struct {
	Skip      bool
	IsSend    bool
	Timestamp int64
	M         messagepb.Message
	Req       *messagepb.Message // this could be nil if not a RPC response
}

type Notification struct {
	From          int64
	State         int32
	Leader        int64
	Zxid          int64
	ElectionEpoch int64
	PeerEpoch     int64
	Version       int32
	Config        string
}

func ParseNotification(id int64, bytes []byte) messagepb.Message {
	pos := int32(0)
	// n := Notification{}
	state := int32(binary.BigEndian.Uint32(bytes[pos : pos+4]))
	pos += 4
	leader := int64(binary.BigEndian.Uint64(bytes[pos : pos+8]))
	pos += 8
	zxid := int64(binary.BigEndian.Uint64(bytes[pos : pos+8]))
	pos += 8
	electionEpoch := int64(binary.BigEndian.Uint64(bytes[pos : pos+8]))
	pos += 8
	peerEpoch := int64(binary.BigEndian.Uint64(bytes[pos : pos+8]))
	pos += 8
	// version := int32(binary.BigEndian.Uint32(bytes[pos : pos+4]))
	pos += 4
	// configLength := int32(binary.BigEndian.Uint32(bytes[pos : pos+4]))
	pos += 4
	// config := string(bytes[pos : pos+configLength])
	if state == 0 {
		return messagepb.Message{Type: messagepb.NOT, From: uint64(id), Vote: messagepb.Vote{ProposedLeader: uint64(leader), ProposedZxid: uint64(zxid), ProposedEpoch: int64(peerEpoch)}, Round: electionEpoch, State: messagepb.Looking}
	}
	if state == 1 {
		return messagepb.Message{Type: messagepb.NOT, From: uint64(id), Vote: messagepb.Vote{ProposedLeader: uint64(leader), ProposedZxid: uint64(zxid), ProposedEpoch: int64(peerEpoch)}, Round: electionEpoch, State: messagepb.Following}
	}
	if state == 2 {
		return messagepb.Message{Type: messagepb.NOT, From: uint64(id), Vote: messagepb.Vote{ProposedLeader: uint64(leader), ProposedZxid: uint64(zxid), ProposedEpoch: int64(peerEpoch)}, Round: electionEpoch, State: messagepb.Leading}
	}
	panic("unknown state")
}

type TxnHeaderT struct {
	ClientId int64
	Cxid     int32
}

func ParseTxnHeader(bytes []byte) TxnHeaderT {
	pos := int32(0)
	ClientId := int64(binary.BigEndian.Uint64(bytes[pos : pos+8]))
	pos += 8
	Cxid := int32(binary.BigEndian.Uint32(bytes[pos : pos+4]))
	pos += 4
	return TxnHeaderT{ClientId: ClientId, Cxid: Cxid}
}

var NOT int32 = 0x10000000
var CLIENTREQ int32 = 0x1000_0001
var CLIENTRESP int32 = 0x1000_0002
var CLIENTRO int32 = 0x1000_0003
var CLIENTRORESP int32 = 0x1000_0004
var BIGSYNC int32 = 0x1000_0005
var TCP int32 = 0x1000_0006
var DIFF int32 = 13
var TRUNC int32 = 14
var SNAP int32 = 15
var OBSERVERINFO int32 = 16
var NEWLEADER int32 = 10
var FOLLOWERINFO int32 = 11
var UPTODATE int32 = 12
var LEADERINFO int32 = 17
var ACKEPOCH int32 = 18
var REQUEST int32 = 1
var PROPOSAL int32 = 2
var ACK int32 = 3
var COMMIT int32 = 4
var PING int32 = 5
var REVALIDATE int32 = 6
var SYNC int32 = 7
var INFORM int32 = 8
var COMMITANDACTIVATE int32 = 9
var INFORMANDACTIVATE int32 = 19
var NONE int32 = -1

type Interpreter struct {
	Bool  [1]byte
	Four  [4]byte
	Eight [8]byte
}

func (intp *Interpreter) ReadBool(reader *bufio.Reader) bool {
	intp.Read(reader, intp.Bool[:])
	return intp.Bool[0] == 1
}

func (intp *Interpreter) ReadUint32(reader *bufio.Reader) uint32 {
	intp.Read(reader, intp.Four[:])
	return binary.BigEndian.Uint32(intp.Four[:])
}

func (intp *Interpreter) ReadUint64(reader *bufio.Reader) uint64 {
	intp.Read(reader, intp.Eight[:])
	return binary.BigEndian.Uint64(intp.Eight[:])
}

func (intp *Interpreter) Read(reader *bufio.Reader, dst []byte) []byte {
	var pos int = 0
	for pos < len(dst) {
		n, err := reader.Read(dst[pos:])
		pos += n
		if err != nil {
			panic(err)
		}
	}

	return dst
}

func (intp Interpreter) SkipString(reader *bufio.Reader) {
	size := int32(intp.ReadUint32(reader))
	if size == -1 {
		return
	}
	reader.Discard(int(size))
}

func (intp Interpreter) SkipAuthInfo(reader *bufio.Reader) {
	VectorSize := int32(intp.ReadUint32(reader))
	if VectorSize == -1 {
		return
	}
	for i := 0; i < int(VectorSize); i++ {
		intp.SkipString(reader)
	}
}

func (intp *Interpreter) GetNextMessage(reader *bufio.Reader) SimConnMessage {
	// SimConnMessageType
	reader.Discard(4) // Discard `SimConnMessageType`
	// timestamp
	Timestamp := int64(intp.ReadUint64(reader))
	// isSend
	IsSend := intp.ReadBool(reader)
	//peerId
	PeerId := int64(intp.ReadUint64(reader))
	// type
	Type := int32(intp.ReadUint32(reader))

	if Type == TCP {
		TCPSize := int32(intp.ReadUint32(reader))
		if TCPSize != 0 {
			panic("TCP message bytes should be 0")
		}
		return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.TCP, From: uint64(PeerId)}}
	}

	switch Type {
	case NOT:
		NotSize := int32(intp.ReadUint32(reader))
		return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: ParseNotification(PeerId, intp.Read(reader, make([]byte, NotSize)))}
	// Client-related
	case CLIENTREQ:
		ClientId := int64(intp.ReadUint64(reader))
		Cxid := int32(intp.ReadUint32(reader))
		reader.Discard(8 + 8 + 4) // Discard `zxid`, `time` and `type`
		return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.CLIENT, From: uint64(PeerId), ReqId: messagepb.ReqIdType{SessionId: ClientId, Cxid: Cxid}}}
	case CLIENTRESP:
		ClientId := int64(intp.ReadUint64(reader))
		Cxid := int32(intp.ReadUint32(reader))
		reader.Discard(8 + 8 + 4) // Discard `zxid`, `time` and `type`
		return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.CLIENTACK, From: uint64(PeerId), ReqId: messagepb.ReqIdType{SessionId: ClientId, Cxid: Cxid}}}
	case CLIENTRO:
		ClientId := int64(intp.ReadUint64(reader))
		Cxid := int32(intp.ReadUint32(reader))
		reader.Discard(8 + 8 + 4) // Discard `zxid`, `time` and `type`
		return SimConnMessage{Skip: true, Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.CLIENTRO, From: uint64(PeerId), ReqId: messagepb.ReqIdType{SessionId: ClientId, Cxid: Cxid}}}
	case CLIENTRORESP:
		ClientId := int64(intp.ReadUint64(reader))
		Cxid := int32(intp.ReadUint32(reader))
		reader.Discard(8 + 8 + 4) // Discard `zxid`, `time` and `type`
		return SimConnMessage{Skip: true, Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.CLIENTRORESP, From: uint64(PeerId), ReqId: messagepb.ReqIdType{SessionId: ClientId, Cxid: Cxid}}}
	}
	// Zab messages
	if Type != BIGSYNC {
		// Type (skipped)
		reader.Discard(4)
		// Zxid
		Zxid := int64(intp.ReadUint64(reader))
		switch Type {
		case PROPOSAL:
			// data
			DataLength := int32(intp.ReadUint32(reader))
			Data := intp.Read(reader, make([]byte, DataLength))
			// first part of Data is TxnHeader
			TxnHeader := ParseTxnHeader(Data)
			intp.SkipAuthInfo(reader)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.PROPOSAL, From: uint64(PeerId), Zxid: uint64(Zxid), ReqId: messagepb.ReqIdType{SessionId: TxnHeader.ClientId, Cxid: TxnHeader.Cxid}}}
		case COMMIT:
			reader.Discard(4 + 4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.COMMIT, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case ACK: // for both ACK and ACK-LD
			reader.Discard(4 + 4)
			zxid := uint64(Zxid)
			if zxid&0xffffffff == 0 {
				return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.ACKLD, From: uint64(PeerId), Zxid: uint64(Zxid)}}
			} else {
				return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.ACK, From: uint64(PeerId), Zxid: uint64(Zxid)}}
			}
		// Discovery
		case FOLLOWERINFO:
			intp.SkipString(reader) // Discard `bsid`
			reader.Discard(4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.FOLLOWERINFO, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case LEADERINFO:
			reader.Discard(4 + 4) // Discard 4-byte buffer for `ver`
			reader.Discard(4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.LEADERINFO, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case ACKEPOCH:
			// epochBytes
			EpochBytesLength := int32(intp.ReadUint32(reader))
			if EpochBytesLength != 4 {
				panic("EpochBytesLength != 4")
			}
			Epoch := int64(int32(intp.ReadUint32(reader)))
			reader.Discard(4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.ACKEPOCH, From: uint64(PeerId), Zxid: uint64(Zxid), Epoch: Epoch}}
		// Synchronization
		case DIFF:
			reader.Discard(4 + 4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.DIFF, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case TRUNC:
			reader.Discard(4 + 4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.TRUNC, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case SNAP:
			reader.Discard(4 + 4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.SNAP, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case NEWLEADER:
			intp.SkipString(reader) // Discard `quorumVerifierBytes`
			reader.Discard(4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.NEWLEADER, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case UPTODATE:
			reader.Discard(4 + 4)
			return SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{Type: messagepb.UPTODATE, From: uint64(PeerId), Zxid: uint64(Zxid)}}
		case PING:
			reader.Discard(4 + 8 + 4 + 4)
			return SimConnMessage{Timestamp: Timestamp, Skip: true}
		default:
			panic(fmt.Sprintf("Unknown Type: %+v", Type))
		}
	} else {
		// qp size
		qpsSize := int32(intp.ReadUint32(reader))
		ret := SimConnMessage{Timestamp: Timestamp, IsSend: IsSend, M: messagepb.Message{From: uint64(PeerId)}}
		for i := 0; i < int(qpsSize); i++ {
			// type
			qpType := int32(intp.ReadUint32(reader))
			Zxid := int64(intp.ReadUint64(reader))
			switch qpType {
			case DIFF:
				reader.Discard(4 + 4)
				ret.M.Type = messagepb.SYNC_DIFF
				ret.M.Zxid = uint64(Zxid)
			case TRUNC:
				reader.Discard(4 + 4)
				ret.M.Type = messagepb.SYNC_TRUNC
				ret.M.Zxid = uint64(Zxid)
			case SNAP:
				reader.Discard(4 + 4)
				ret.M.Type = messagepb.SYNC_SNAP
				ret.M.Zxid = uint64(Zxid)
			case PROPOSAL:
				DataLength := int32(intp.ReadUint32(reader))
				Data := intp.Read(reader, make([]byte, DataLength))
				// first part of Data is TxnHeader
				TxnHeader := ParseTxnHeader(Data)
				intp.SkipAuthInfo(reader)
				ret.M.Proposals = append(ret.M.Proposals, messagepb.Proposal{Zxid: uint64(Zxid), ReqId: messagepb.ReqIdType{SessionId: TxnHeader.ClientId, Cxid: TxnHeader.Cxid}})
			case COMMIT:
				reader.Discard(4 + 4)
				ret.M.Commits = append(ret.M.Commits, uint64(Zxid))
			case NEWLEADER:
				intp.SkipString(reader) // Discard `quorumVerifierBytes`
				reader.Discard(4)
				ret.M.NewleaderZxid = uint64(Zxid)
			default:
				panic(fmt.Sprintf("Unknown qpType: %+v", qpType))
			}
		}
		return ret
	}
}
