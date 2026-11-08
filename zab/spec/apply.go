package spec

import (
	"sort"

	"go.nyu.edu/protosim/zab/messagepb"
)

func Apply(s State, msg *messagepb.Message) State {
	switch msg.Type {
	case messagepb.TCP:
		s.TCPConnect(msg)
	case messagepb.TIMEOUT:
		s.Timeout()
	case messagepb.TURNTOLEADER:
		s.TurnToLeading()
	case messagepb.TURNTOFOLLOWER:
		s.TurnToFollowing()
	case messagepb.FOLLOWERINFO:
		s.FollowerInfoHandler(msg)
	case messagepb.LEADERINFO:
		s.LeaderInfoHandler(msg)
	case messagepb.ACKEPOCH:
		s.AckEpochHandler(msg)
	case messagepb.DIFF:
		s.SyncHandler(msg)
	case messagepb.TRUNC:
		s.SyncHandler(msg)
	case messagepb.NEWLEADER:
		s.NewLeaderHandler(msg)
	case messagepb.ACKLD:
		s.ACKLDHandler(msg)
	case messagepb.UPTODATE:
		s.UpToDateHandler(msg)
	case messagepb.CLIENT:
		s.RequestHandler(msg)
	case messagepb.PROPOSAL:
		s.ProposalHandler(msg)
	case messagepb.ACK:
		s.ACKHandler(msg)
	case messagepb.COMMIT:
		s.CommitHandler(msg)
	}
	return s
}

func (s *State) TCPConnect(m *messagepb.Message) {
	if s.ElectionStates.State == Leading {
		if IsMyLearner(m.From, s.ElectionStates.Learners) {
			return
		}
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		s.ElectionStates.Learners = updateConnectingOrAckldRecv(s.ElectionStates.Learners, m.From)
	}
	if s.ElectionStates.State == Following {
		if IsMyLeader(m.From, s.ElectionStates.ConnectInfo) {
			return
		}
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		s.ElectionStates.ConnectInfo = &ConnectInfo{Sid: m.From, NlRcv: false}
	}
}

func (s *State) Timeout() {
	s.ElectionStates = &DiscoverVars{}
	s.PacketsSync = &SyncVars{}
	s.ElectionStates.State = Looking
	s.ZabState = ELECTION
	s.LastProcessed = s.GetLastZxid()
	s.ElectionStates.Connecting = &[]uint64{}
	s.ElectionStates.Election = make(map[uint64]AckEpochInfo)
	s.ElectionStates.ConnectInfo = &ConnectInfo{Sid: NOONE, NlRcv: false}
	s.ElectionStates.Learners = &[]uint64{}
	s.ElectionStates.AckLdRecv = &[]uint64{}
}

func (s *State) TurnToLeading() {
	if s.ElectionStates.State == Looking {
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		s.ElectionStates.State = Leading
		s.ZabState = DISCOVERY
		s.ElectionStates.Connecting = &[]uint64{s.Me}
		s.ElectionStates.Election = make(map[uint64]AckEpochInfo)
		s.ElectionStates.Election[s.Me] = AckEpochInfo{LastProcessed: s.LastProcessed}
		s.ElectionStates.Learners = &[]uint64{}
		//add self into learners
		*s.ElectionStates.Learners = append(*s.ElectionStates.Learners, s.Me)
		s.ElectionStates.AckLdRecv = &[]uint64{}
		//add self into ackldrecv
		*s.ElectionStates.AckLdRecv = append(*s.ElectionStates.AckLdRecv, s.Me)
		s.ElectionStates.tempMaxEpoch = s.AcceptEpoch + 1
	}
}

func (s *State) TurnToFollowing() {
	if s.ElectionStates.State == Looking {
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		s.PacketsSync = &SyncVars{}
		s.ElectionStates.State = Following
		s.ZabState = DISCOVERY
	}
}

func (s *State) FollowerInfoHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Leading && IsMyLearner(m.From, s.ElectionStates.Learners) {
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		msg_epoch := int64(m.Zxid >> 32)
		if s.waitingForNewEpoch(*s.ElectionStates.Connecting) && s.ZabState == DISCOVERY {
			if msg_epoch >= s.ElectionStates.tempMaxEpoch {
				s.ElectionStates.tempMaxEpoch = msg_epoch + 1
			}
			s.ElectionStates.Connecting = updateConnectingOrAckldRecv(s.ElectionStates.Connecting, m.From)
			if !s.waitingForNewEpoch(*s.ElectionStates.Connecting) {
				s.AcceptEpoch = s.ElectionStates.tempMaxEpoch
			}
		} else {
			s.ElectionStates.Connecting = updateConnectingOrAckldRecv(s.ElectionStates.Connecting, m.From)
		}
	}
}

func (s *State) LeaderInfoHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Following && IsMyLeader(m.From, s.ElectionStates.ConnectInfo) {
		newEpoch := int64(m.Zxid >> 32)
		epochOK := newEpoch >= s.AcceptEpoch
		stateOK := s.ZabState == DISCOVERY
		if epochOK {
			if stateOK {
				if newEpoch > s.AcceptEpoch {
					s.AcceptEpoch = newEpoch
				}
				s.ZabState = SYNC
			}
		} else {
			s.Timeout()
		}
	}
}

func (s *State) AckEpochHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Leading && IsMyLearner(m.From, s.ElectionStates.Learners) {
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		leaderStateSummary := StateSummary{Epoch: s.Epoch, Zxid: s.GetLastZxid()}
		followerStateSummary := StateSummary{Epoch: m.Epoch, Zxid: m.Zxid}
		logOk := !IsMoreRecentThen(followerStateSummary, leaderStateSummary)
		electing_quorum := countElectionQuorum(s.ElectionStates.Election)
		if s.ElectionFinished(electing_quorum) {
			s.ElectionStates.Election = updateElection(s.ElectionStates.Election, m.From, m.Zxid)
		} else if s.ZabState == DISCOVERY {
			if logOk {
				s.ElectionStates.Election = updateElection(s.ElectionStates.Election, m.From, m.Zxid)
				new_electing_quorum := countElectionQuorum(s.ElectionStates.Election)
				if s.ElectionFinished(new_electing_quorum) {
					s.LeaderTurnToSynchronization()
				}
			} else {
				s.Timeout()
			}
		}
	}
}

func (s *State) SyncHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Following && IsMyLeader(m.From, s.ElectionStates.ConnectInfo) && s.ZabState == SYNC {
		if m.Type == messagepb.DIFF {
			return
		}
		if m.Type == messagepb.TRUNC {
			truncZxid := m.Zxid
			truncIndex := s.ZxidToIndex(s.Log, truncZxid)
			if truncIndex != -1 {
				//copy on write on s.log
				newLog := make([]*Command, truncIndex+1)
				copy(newLog, s.Log)
				s.Log = newLog
				s.LastCommitted = truncZxid
				s.LastProcessed = truncZxid
			}
		}
	}
}

func (s *State) NewLeaderHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Following && IsMyLeader(m.From, s.ElectionStates.ConnectInfo) {
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		s.Epoch = s.AcceptEpoch
		s.ElectionStates.ConnectInfo = &ConnectInfo{Sid: m.From, NlRcv: true}
		packetsInSync := s.PacketsSync.Uncommitted
		s.Log = appendToLog(s.Log, packetsInSync)
		s.PacketsSync = &SyncVars{}
		s.PacketsSync.Uncommitted = []*Command{}
	}
}

func (s *State) ACKLDHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Leading && IsMyLearner(m.From, s.ElectionStates.Learners) {
		if s.ZabState == DISCOVERY {
			return
		}
		s.UpdateAcksid(m.From, m.Zxid)
		s.ElectionStates = copyDiscoverVars(s.ElectionStates)
		if int64(m.Zxid>>32) == s.AcceptEpoch {
			s.ElectionStates.AckLdRecv = updateConnectingOrAckldRecv(s.ElectionStates.AckLdRecv, m.From)
			// The leader commits and starts broadcasting once, when a quorum
			// first acknowledged NEWLEADER. Later acknowledgments (a learner
			// joining late, or the ACK a follower sends after UPTODATE, which
			// has the same zxid form) do not commit outstanding proposals.
			if s.ZabState != BROADCAST && s.InQuorum(*s.ElectionStates.AckLdRecv) {
				s.LeaderTurnToBroadcast()
			}
		}
	}
}

func (s *State) UpToDateHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Following && IsMyLeader(m.From, s.ElectionStates.ConnectInfo) {
		s.ZabState = BROADCAST
	}
}

func (s *State) RequestHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Leading && s.ZabState == BROADCAST {
		newTxn := Command{CommandIndex: IncZxid(s.Epoch, s.GetLastZxid()), Command: Op{Type: EntryNormal, ReqID: ReqIndex{Session: m.ReqId.SessionId, Cxid: m.ReqId.Cxid}}, CommandTerm: s.AcceptEpoch, AckId: []uint64{s.Me}}
		new_log := make([]*Command, len(s.Log)+1)
		copy(new_log, s.Log)
		new_log[len(s.Log)] = &newTxn
		s.Log = new_log
	}
}

func (s *State) ProposalHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Following && IsMyLeader(m.From, s.ElectionStates.ConnectInfo) && IsNextZxid(s.LastQueued(), m.Zxid) {
		newTxn := Command{CommandIndex: m.Zxid, Command: Op{Type: EntryNormal, ReqID: ReqIndex{Session: m.ReqId.SessionId, Cxid: m.ReqId.Cxid}}, CommandTerm: s.AcceptEpoch, AckId: []uint64{m.From}}
		if s.ZabState == SYNC {
			s.PacketsSync = copySyncVars(s.PacketsSync)
			s.PacketsSync.Uncommitted = appendToLog(s.PacketsSync.Uncommitted, []*Command{&newTxn})
		} else if s.ZabState == BROADCAST {
			s.Log = appendToLog(s.Log, []*Command{&newTxn})
		}
	}
}

func (s *State) ACKHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Leading && s.ZabState == BROADCAST && IsMyLearner(m.From, s.ElectionStates.Learners) {
		outstanding := s.LastCommitted < s.GetLastZxid()
		hasCommitted := !(m.Zxid > s.LastCommitted)
		index := s.ZxidToIndex(s.Log, m.Zxid)
		exists := index >= 0
		// the monoticallyInc is defined in tla but not used in implementation, so I comment it out here
		ackIndex := s.LastAckIndexFromFollower(m.From)
		monoticallyInc := ackIndex == -1 || ackIndex+1 == index
		if exists && monoticallyInc {
			//copy a new log, add this follower into the entry at index
			if inSlice(s.Log[index].AckId, m.From) != -1 {
				return
			}
			newLog := make([]*Command, len(s.Log))
			copy(newLog, s.Log)
			newEntry := &Command{CommandIndex: s.Log[index].CommandIndex, Command: s.Log[index].Command, CommandTerm: s.Log[index].CommandTerm}
			newLog[index] = newEntry
			newLog[index].AckId = make([]uint64, len(s.Log[index].AckId)+1)
			copy(newLog[index].AckId, s.Log[index].AckId)
			newLog[index].AckId[len(s.Log[index].AckId)] = m.From
			s.Log = newLog
			if outstanding && !hasCommitted {
				s.LeaderTryToCommit(index, m.Zxid)
			}
		}
	}
}

func (s *State) CommitHandler(m *messagepb.Message) {
	if s.ElectionStates.State == Following && IsMyLeader(m.From, s.ElectionStates.ConnectInfo) {
		if s.ZabState == SYNC {
			committedIndex, committedZxid := s.StateLastCommitted()
			nextIndex := committedIndex + 1
			exist := (nextIndex < len(s.Log)+len(s.PacketsSync.Uncommitted)) && IsNextZxid(committedZxid, m.Zxid)
			match := s.fetchNextUncommitted(nextIndex).CommandIndex == m.Zxid
			if exist && match {
				if nextIndex >= len(s.Log) {
					committedTxn := s.PacketsSync.Uncommitted[nextIndex-len(s.Log)]
					s.Log = appendToLog(s.Log, []*Command{committedTxn})
					s.LastCommitted = committedTxn.CommandIndex
					s.LastProcessed = committedTxn.CommandIndex
					s.PacketsSync = copySyncVars(s.PacketsSync)
					_, s.PacketsSync.Uncommitted = popFromSyncUncommitted(s.PacketsSync.Uncommitted)
				} else {
					s.LastCommitted = m.Zxid
					s.LastProcessed = m.Zxid
				}
			}
		} else if s.ZabState == BROADCAST {
			if s.LastCommitted < s.GetLastZxid() {
				index := s.ZxidToIndex(s.Log, s.LastCommitted)
				firstElementZxid := s.Log[index+1].CommandIndex
				if firstElementZxid == m.Zxid {
					s.LastCommitted = firstElementZxid
					s.LastProcessed = firstElementZxid
				}
			}
		}
	}
}

func IsMyLeader(leader uint64, conninfo *ConnectInfo) bool {
	if conninfo == nil {
		return false
	}
	return leader == conninfo.Sid
}

func IsMyLearner(follower uint64, Learners *[]uint64) bool {
	return inSlice(*Learners, follower) != -1
}

func copyDiscoverVars(vars *DiscoverVars) *DiscoverVars {
	//shadow copy vars to return value
	newvars := DiscoverVars{}
	newvars.State = vars.State
	// newvars.LogicalClock = vars.LogicalClock
	// newvars.Epoch = vars.Epoch
	// newvars.CurrentVote = vars.CurrentVote
	// newvars.ReceivedVotes = vars.ReceivedVotes
	// newvars.OutOfElection = vars.OutOfElection
	newvars.Connecting = vars.Connecting
	newvars.Election = vars.Election
	newvars.tempMaxEpoch = vars.tempMaxEpoch
	newvars.ConnectInfo = vars.ConnectInfo
	newvars.Learners = vars.Learners
	newvars.AckLdRecv = vars.AckLdRecv
	return &newvars
}

func updateConnectingOrAckldRecv(oldset *[]uint64, id uint64) *[]uint64 {
	//add id into oldset, deduplicate, return newset
	pos := inSlice(*oldset, id)
	if pos == -1 {
		newset := make([]uint64, len(*oldset)+1)
		copy(newset, *oldset)
		newset[len(*oldset)] = id
		sort.Slice(newset, func(i, j int) bool { return newset[i] < newset[j] })
		return &newset
	}
	return oldset
}

func (s *State) GetLastZxid() uint64 {
	return s.Log[len(s.Log)-1].CommandIndex
}

func (s *State) waitingForNewEpoch(sid_connecting []uint64) bool {
	return !s.InQuorum(sid_connecting)
}

func IsMoreRecentThen(ss1 StateSummary, ss2 StateSummary) bool {
	return ss1.Epoch > ss2.Epoch || (ss1.Epoch == ss2.Epoch && ss1.Zxid > ss2.Zxid)
}

func countElectionQuorum(m map[uint64]AckEpochInfo) []uint64 {
	//return all ids in m that has value true
	ids := []uint64{}
	for k, _ := range m {
		ids = append(ids, k)
	}
	return ids
}

func (s *State) ElectionFinished(sid_election []uint64) bool {
	return s.InQuorum(sid_election)
}

func updateElection(oldset map[uint64]AckEpochInfo, id uint64, lastProcessed uint64) map[uint64]AckEpochInfo {
	v, ok := oldset[id]
	if ok {
		if lastProcessed == v.LastProcessed {
			return oldset
		}
	}
	newset := make(map[uint64]AckEpochInfo)
	for k, v := range oldset {
		newset[k] = v
	}
	newset[id] = AckEpochInfo{LastProcessed: lastProcessed}
	return newset
}

func (s *State) LeaderTurnToSynchronization() {
	s.Epoch = s.AcceptEpoch
	s.ZabState = SYNC
}

func appendToLog(log []*Command, newTxn []*Command) []*Command {
	//copy a new log, append newTxn into it
	newLog := make([]*Command, len(log)+len(newTxn))
	copy(newLog, log)
	copy(newLog[len(log):], newTxn)
	return newLog
}

func (s *State) UpdateAcksid(from uint64, zxid uint64) {
	//add from into ackId in all entries that has zxid <= zxid
	new_log := make([]*Command, len(s.Log))
	copy(new_log, s.Log)
	for i, v := range s.Log {
		if v.CommandIndex <= zxid {
			if inSlice(v.AckId, from) == -1 {
				new_entry := &Command{CommandIndex: v.CommandIndex, Command: v.Command, CommandTerm: v.CommandTerm}
				new_entry.AckId = make([]uint64, len(v.AckId)+1)
				copy(new_entry.AckId, v.AckId)
				new_entry.AckId[len(v.AckId)] = from
				new_log[i] = new_entry
			}
		}
	}
	s.Log = new_log
}

func (s *State) LeaderTurnToBroadcast() {
	s.LastCommitted = s.GetLastZxid()
	s.LastProcessed = s.GetLastZxid()
	s.ZabState = BROADCAST
}

func IncZxid(local_epoch int64, zxid uint64) uint64 {
	lepoch := uint64(local_epoch)
	epoch := zxid >> 32
	index := zxid & 0xffffffff
	if lepoch == epoch {
		return epoch<<32 | index + 1
	} else {
		return lepoch<<32 | 1
	}
}

func (s *State) LastQueued() uint64 {
	if s.ElectionStates.State != Following || s.ZabState != SYNC {
		return s.GetLastZxid()
	}
	if len(s.PacketsSync.Uncommitted) == 0 {
		return s.GetLastZxid()
	}
	return s.PacketsSync.Uncommitted[len(s.PacketsSync.Uncommitted)-1].CommandIndex
}

func copySyncVars(vars *SyncVars) *SyncVars {
	//shadow copy vars to return value
	newvars := SyncVars{}
	newvars.Uncommitted = vars.Uncommitted
	return &newvars
}

func (s *State) LastAckIndexFromFollower(follower uint64) int {
	//return the index of the last entry in s.Log that follower is in the acklist
	last_one := -1
	for i, v := range s.Log {
		if inSlice(v.AckId, follower) != -1 {
			last_one = i
		}
	}
	return last_one
}

func (s *State) LeaderTryToCommit(index int, zxid uint64) {
	hasAllQuorums := s.InQuorum(s.Log[index].AckId)
	if hasAllQuorums {
		s.LastCommitted = zxid
		s.LastProcessed = zxid
	}
}

func (s *State) StateLastCommitted() (int, uint64) {
	if s.ZabState == BROADCAST {
		return s.ZxidToIndex(s.Log, s.LastCommitted), s.LastCommitted
	}
	if s.ElectionStates.State == Leading {
		return len(s.Log) - 1, s.GetLastZxid()
	} else {
		return s.ZxidToIndex(s.Log, s.LastCommitted), s.LastCommitted
	}
}

func (s *State) fetchNextUncommitted(index int) *Command {
	if index >= len(s.Log) {
		return s.PacketsSync.Uncommitted[index-len(s.Log)]
	} else {
		return s.Log[index]
	}
}

func popFromSyncUncommitted(uncommitted []*Command) (*Command, []*Command) {
	//copy a new uncommitted, pop the first element from it
	newUncommitted := make([]*Command, len(uncommitted)-1)
	copy(newUncommitted, (uncommitted)[1:])
	return (uncommitted)[0], newUncommitted
}
