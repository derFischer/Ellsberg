package spec

import "math"

type StateType int8

const (
	StateUnknown StateType = 0
	Leading      StateType = 1
	Following    StateType = 2
	Looking      StateType = 3
)

type ZabStateType int8

const (
	ZabStateUnknown ZabStateType = 0
	DISCOVERY       ZabStateType = 1
	SYNC            ZabStateType = 2
	BROADCAST       ZabStateType = 4
	ELECTION        ZabStateType = 8
)

type SyncType int8

const (
	DIFF  SyncType = 0
	SNAP  SyncType = 1
	TRUNC SyncType = 2
)

const Unknown uint64 = 0
const VotesUnknown uint8 = 2

type EntryType int8

const (
	EntryNormal     EntryType = 0
	EntryKVPut      EntryType = 1
	EntryKVRO       EntryType = 2
	EntryConfChange EntryType = 3
	EntryEmpty      EntryType = 4
	EntryUnknown    EntryType = 5
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

type ReqIndex struct {
	Session int64
	Cxid    int32
}

type Op struct {
	ReqID interface{} `dfyType:"uint64"`
	Type  EntryType
}

type Command struct {
	CommandIndex uint64 `dfyDefault:"0"`
	Command      Op     `dfyDefault:"Op(0, EntryUnknown, [], [], [], [])"`
	CommandTerm  int64  `dfyDefault:"0"`
	AckId        []uint64
}

type Vote struct {
	ProposedLeader uint64
	ProposedZxid   uint64
	ProposedEpoch  int64
}

type ConnectInfo struct {
	Sid   uint64
	NlRcv bool
}

const (
	TRUNCATED = math.MaxUint64
	NONEXIST  = math.MaxUint64 - 1
)

const NOONE = math.MaxUint64 - 1

type StateSummary struct {
	Epoch int64
	Zxid  uint64
}

type AckEpochInfo struct {
	LastProcessed uint64
}

type DiscoverVars struct {
	State StateType
	// Epoch        int64
	Connecting   *[]uint64
	Election     map[uint64]AckEpochInfo
	tempMaxEpoch int64
	ConnectInfo  *ConnectInfo
	Learners     *[]uint64
	AckLdRecv    *[]uint64
}

type SyncVars struct {
	Uncommitted []*Command
}

type State struct {
	Me             uint64
	ElectionStates *DiscoverVars
	ZabState       ZabStateType
	Epoch          int64
	AcceptEpoch    int64
	LastCommitted  uint64
	LastProcessed  uint64
	Peers          []uint64
	Log            []*Command
	PacketsSync    *SyncVars
}
