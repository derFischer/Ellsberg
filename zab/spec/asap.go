package spec

import (
	"go.nyu.edu/protosim/zab/messagepb"
)

// ApplyASAP applies msg, the next message of its connection, to s if it can be
// applied right away without losing behaviors, and reports whether it did.
func ApplyASAP(msg *messagepb.Message, s *State) bool {
	switch msg.Type {
	case messagepb.ACK:
		index := s.ZxidToIndex(s.Log, msg.Zxid)
		prev_applied := false
		if index > 0 {
			for _, sid := range s.Log[index-1].AckId {
				if sid == msg.From {
					prev_applied = true
				}
			}
		}
		if prev_applied || msg.Zxid <= s.LastCommitted {
			s.ACKHandler(msg)
			// PrintState(state)
			return true
		}
	case messagepb.ACKLD:
		if int64(msg.Zxid>>32) < s.AcceptEpoch || (s.ElectionStates.AckLdRecv != nil && s.InQuorum(*s.ElectionStates.AckLdRecv)) {
			s.ACKLDHandler(msg)
			return true
		}
	}
	return false
}
