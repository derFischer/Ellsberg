package statepb

import "math"

type StateType int32

const (
	Leader       StateType = 0
	Follower     StateType = 1
	Candidate    StateType = 2
	StateUnknown StateType = 3
)

const Unknown = math.MaxUint64
const VotesUnknown = 2

type EntryType int32

const (
	EntryNormal     EntryType = 0
	EntryKVPut      EntryType = 1
	EntryKVRO       EntryType = 2
	EntryConfChange EntryType = 3
	EntryUnknown    EntryType = 4
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

type Op struct {
	ReqID      interface{}
	Type       EntryType
	Key        []byte
	Value      []byte
	Data       []byte
	ConfChange []ReconfigChange
}

type Command struct {
	CommandIndex uint64
	Command      Op
	CommandTerm  uint64
}
