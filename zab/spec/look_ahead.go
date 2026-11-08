package spec

import mpb "go.nyu.edu/protosim/zab/messagepb"

var Look_ahead_index map[mpb.MessageType]int

func Look_ahead(mpb.MessageType) mpb.MessageType {
	return mpb.NONE
}
