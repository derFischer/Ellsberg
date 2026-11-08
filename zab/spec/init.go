package spec

// InitialState returns the state of server id when it starts: LOOKING, with an
// empty log.
func InitialState(id uint64, peers []uint64) State {
	log := []*Command{{CommandIndex: 0, Command: Op{Type: EntryNormal}, AckId: []uint64{id}}}
	var ackepochinfo map[uint64]AckEpochInfo
	return State{Me: id, ElectionStates: &DiscoverVars{State: Looking, Connecting: &[]uint64{}, Election: ackepochinfo, ConnectInfo: &ConnectInfo{Sid: NOONE, NlRcv: false}, Learners: &[]uint64{}, AckLdRecv: &[]uint64{}}, ZabState: ELECTION, AcceptEpoch: 0, LastCommitted: 0, Peers: peers, Log: log}
}
