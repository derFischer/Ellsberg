package spec

import (
	"math"
)

type StateType int8

const (
	Leader       StateType = 0
	Follower     StateType = 1
	Candidate    StateType = 2
	StateUnknown StateType = 3
)

type ReplyType int8

const (
	NotReply  ReplyType = 0
	AppReply  ReplyType = 1
	VoteReply ReplyType = 2
)

const Unknown uint64 = math.MaxUint64
const VotesUnknown uint8 = 2

const (
	TRUNCATED = math.MaxUint64
	NONEXIST  = math.MaxUint64 - 1
)

const NOONE = math.MaxUint64 - 1

type Connection struct {
	Ids []uint64
}

type MajorityQuorum []uint64

type JointQuorum [2]MajorityQuorum
