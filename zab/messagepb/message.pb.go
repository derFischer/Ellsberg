package messagepb

// type MsgType int8
// type Ctx interface{}

// const (
// 	ClientRequest  MsgType = 1
// 	ClientRespSucc MsgType = 2
// 	ClientRespFail MsgType = 3
// 	RaftRequest    MsgType = 4
// 	RaftResp       MsgType = 5
// )

type MessageType uint32

// const (
// 	NONE MessageType = 0
// 	//raft messages
// 	MsgApp      MessageType = 1
// 	MsgAppResp  MessageType = 2
// 	MsgVote     MessageType = 3
// 	MsgVoteResp MessageType = 4
// 	// MsgReadIndex     MessageType = 5
// 	// MsgReadIndexResp MessageType = 6

// 	//client messages
// 	ClientNormal     MessageType = 7
// 	ClientKVPut      MessageType = 8
// 	ClientKVRO       MessageType = 9
// 	ClientConfChange MessageType = 10

// 	ClientNormalResp MessageType = 11
// 	ClientROResp     MessageType = 12

// 	//nil message
// 	CheckLeader      MessageType = 13
// 	CheckCommitIndex MessageType = 14
// 	Timeout          MessageType = 15
// )

var Send_Message_Types = []MessageType{FOLLOWERINFO, LEADERINFO, ACKEPOCH, NEWLEADER, UPTODATE, SYNC_DIFF, SYNC_TRUNC, SYNC_SNAP, PROPOSAL, ACK, COMMIT, CLIENTACK, CLIENTRORESP, ACKLD, TCP}

const (
	NONE MessageType = 0
	NOT  MessageType = 1 << 0
	// Discovery
	FOLLOWERINFO MessageType = 1 << 1
	LEADERINFO   MessageType = 1 << 2
	ACKEPOCH     MessageType = 1 << 3
	// Synchronization
	DIFF       MessageType = 1 << 4
	TRUNC      MessageType = 1 << 5
	SNAP       MessageType = 1 << 6
	NEWLEADER  MessageType = 1 << 7
	UPTODATE   MessageType = 1 << 8
	SYNC_DIFF  MessageType = 1 << 9
	SYNC_TRUNC MessageType = 1 << 10
	SYNC_SNAP  MessageType = 1 << 11

	CLIENT       MessageType = 1 << 12
	CLIENTRO     MessageType = 1 << 13
	PROPOSAL     MessageType = 1 << 14
	ACK          MessageType = 1 << 15
	COMMIT       MessageType = 1 << 16
	CLIENTACK    MessageType = 1 << 17
	CLIENTRORESP MessageType = 1 << 18
	ACKLD        MessageType = 1 << 19
	//nil message
	// TIMEOUT     MessageType = 16
	// CHECKQUORUM MessageType = 17
	TIMEOUT                    = 1 << 20
	TURNTOLEADER   MessageType = 1 << 21
	TURNTOFOLLOWER MessageType = 1 << 22

	TCP MessageType = 1 << 23
)

type ReqIdType struct {
	SessionId int64
	Cxid      int32
}

type Proposal struct {
	Zxid  uint64
	ReqId ReqIdType
}

type StateType int8

const (
	StateUnknown StateType = 0
	Leading      StateType = 1
	Following    StateType = 2
	Looking      StateType = 3
)

type Vote struct {
	ProposedLeader uint64
	ProposedZxid   uint64
	ProposedEpoch  int64
}

type Message struct {
	Type  MessageType
	From  uint64
	Vote  Vote
	Round int64
	State StateType
	Epoch int64
	Zxid  uint64
	ReqId ReqIdType
	// For BIGSYNC
	NewleaderZxid uint64
	Proposals     []Proposal
	Commits       []uint64
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

// func (m *Message) MarshalJSON() ([]byte, error) {
// 	type MessageAlias Message
// 	tmp := &struct {
// 		*MessageAlias
// 		Ctx   nilOrCtxArray
// 		ReqID nilOrUint64
// 	}{
// 		MessageAlias: (*MessageAlias)(m),
// 	}
// 	if m.Ctx == nil {
// 		tmp.Ctx = nilOrCtxArray{false, [8]uint8{}}
// 	} else {
// 		tmp.Ctx = nilOrCtxArray{true, m.Ctx.([8]uint8)}
// 	}
// 	if m.ReqID == nil {
// 		tmp.ReqID = nilOrUint64{false, 0}
// 	} else {
// 		tmp.ReqID = nilOrUint64{true, m.ReqID.(uint64)}
// 	}
// 	return json.Marshal(tmp)
// }

// func (m *Message) UnmarshalJSON(data []byte) error {
// 	type MessageAlias Message

// 	tmp := &struct {
// 		*MessageAlias
// 		Ctx   nilOrCtxArray
// 		ReqID nilOrUint64
// 	}{
// 		MessageAlias: (*MessageAlias)(m),
// 	}
// 	err := json.Unmarshal(data, tmp)
// 	if err != nil {
// 		return err
// 	}
// 	if tmp.Ctx.Valid {
// 		m.Ctx = tmp.Ctx.Value
// 	} else {
// 		m.Ctx = nil
// 	}
// 	if tmp.ReqID.Valid {
// 		m.ReqID = tmp.ReqID.Value
// 	} else {
// 		m.ReqID = nil
// 	}
// 	return nil
// }
