package spec

import mpb "go.nyu.edu/protosim/raft/messagepb"

var Look_ahead_index map[mpb.MessageType]int

func Look_ahead(mt mpb.MessageType) mpb.MessageType {
	if mt == mpb.ClientNormalResp || mt == mpb.ClientROResp {
		return mpb.MsgApp | mpb.MsgAppResp | mpb.MsgVote | mpb.MsgVoteResp
	}
	return mpb.NONE
}
