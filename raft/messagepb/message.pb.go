package messagepb

import (
	"encoding/json"
)

type MsgType int8
type Ctx interface{}

const (
	ClientRequest  MsgType = 1
	ClientRespSucc MsgType = 2
	ClientRespFail MsgType = 3
	RaftRequest    MsgType = 4
	RaftResp       MsgType = 5
)

type MessageType uint32

// this only counts the types of messages that might be sent by a node (client messages and internal signals not included)
var Send_Message_Types = []MessageType{MsgApp, MsgAppResp, MsgVote, MsgVoteResp, ClientNormalResp, ClientROResp}

const (
	NONE MessageType = 0
	//raft messages
	MsgApp      MessageType = 1 << 0
	MsgAppResp  MessageType = 1 << 1
	MsgVote     MessageType = 1 << 2
	MsgVoteResp MessageType = 1 << 3
	// MsgReadIndex     MessageType = 5
	// MsgReadIndexResp MessageType = 6

	//client messages
	ClientNormal     MessageType = 1 << 4
	ClientKVPut      MessageType = 1 << 5
	ClientKVRO       MessageType = 1 << 6
	ClientConfChange MessageType = 1 << 7

	ClientNormalResp MessageType = 1 << 8
	ClientROResp     MessageType = 1 << 9

	//nil message
	CheckLeader      MessageType = 1 << 10
	CheckCommitIndex MessageType = 1 << 11
	Timeout          MessageType = 1 << 12
)

type ReconfigChangeType int8

const (
	AddNode    ReconfigChangeType = 1
	RemoveNode ReconfigChangeType = 2
)

type ReconfigChange struct {
	Type ReconfigChangeType
	Node uint64
}

type EntryType int8

const (
	EntryNormal     EntryType = 0
	EntryKVPut      EntryType = 1
	EntryKVRO       EntryType = 2
	EntryConfChange EntryType = 3
	EntryEmpty      EntryType = 4
	EntryUnknown    EntryType = 5
)

type Op struct {
	ReqID      interface{} `dfyType:"uint64"`
	Type       EntryType
	ConfChange []ReconfigChange
}

type Command struct {
	CommandIndex uint64 `dfyDefault:"0"`
	Command      Op     `dfyDefault:"Op(0, EntryUnknown, [], [], [], [])"`
	CommandTerm  uint64 `dfyDefault:"0"`
}

type Message struct {
	Type MessageType `dfyDefault:"MsgApp"`

	//raft message
	From        uint64    `dfyDefault:"0"`
	To          uint64    `dfyDefault:"0"`
	Mterm       uint64    `dfyDefault:"0"`
	Index       uint64    `dfyDefault:"0"`
	IndexTerm   uint64    `dfyDefault:"0"`
	Success     bool      `dfyDefault:"false"`
	MEntries    []Command `dfyDefault:"[]"`
	CommitIndex uint64    `dfyDefault:"0"`
	MatchIndex  uint64    `dfyDefault:"0"`
	ReqTerm     uint64    `dfyDefault:"0"`
	Ctx         Ctx

	//client messages
	ReqID      interface{}      `dfyType:"uint64" dfyDefault:"0"`
	ConfChange []ReconfigChange `dfyDefault:"[]"`
}

// func (m *Message) Equal(m2 *Message) bool {
// 	if m.Type != m2.Type {
// 		return false
// 	}

// 	switch m.Type {
// 	case MsgAppResp:
// 		return m.From == m2.From && m.To == m2.To && m.Mterm == m2.Mterm && m.Index == m2.Index && m.Success == m2.Success && reflect.DeepEqual(m.Ctx, m2.Ctx)

// 	}
// }

// ////////////////////
// JSON
// ////////////////////
type nilOrUint64 struct {
	Valid bool
	Value uint64
}

type nilOrCtxArray struct {
	Valid bool
	Value [8]uint8
}

func (m *Message) MarshalJSON() ([]byte, error) {
	type MessageAlias Message
	tmp := &struct {
		*MessageAlias
		Ctx   nilOrCtxArray
		ReqID nilOrUint64
	}{
		MessageAlias: (*MessageAlias)(m),
	}
	if m.Ctx == nil {
		tmp.Ctx = nilOrCtxArray{false, [8]uint8{}}
	} else {
		tmp.Ctx = nilOrCtxArray{true, m.Ctx.([8]uint8)}
	}
	if m.ReqID == nil {
		tmp.ReqID = nilOrUint64{false, 0}
	} else {
		tmp.ReqID = nilOrUint64{true, m.ReqID.(uint64)}
	}
	return json.Marshal(tmp)
}

func (m *Message) UnmarshalJSON(data []byte) error {
	type MessageAlias Message

	tmp := &struct {
		*MessageAlias
		Ctx   nilOrCtxArray
		ReqID nilOrUint64
	}{
		MessageAlias: (*MessageAlias)(m),
	}
	err := json.Unmarshal(data, tmp)
	if err != nil {
		return err
	}
	if tmp.Ctx.Valid {
		m.Ctx = tmp.Ctx.Value
	} else {
		m.Ctx = nil
	}
	if tmp.ReqID.Valid {
		m.ReqID = tmp.ReqID.Value
	} else {
		m.ReqID = nil
	}
	return nil
}
