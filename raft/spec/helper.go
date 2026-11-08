package spec

import (
	"sort"

	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func (s *State) GetFirstCommandIndex() uint64 {
	return s.Log[0].CommandIndex
}

func (s *State) GetLastCommandIndex() uint64 {
	return s.Log[len(s.Log)-1].CommandIndex
}

func (s *State) GetLastCommandTerm() uint64 {
	return s.Log[len(s.Log)-1].CommandTerm
}

func (s *State) GetCommand(commandIndex uint64) mpb.Command {
	localIndex := s.GetCurrentIndex(commandIndex)
	if localIndex == TRUNCATED {
		return mpb.Command{CommandIndex: TRUNCATED, CommandTerm: TRUNCATED}
	} else if localIndex == NONEXIST {
		return mpb.Command{CommandIndex: NONEXIST, CommandTerm: NONEXIST}
	}
	return s.Log[localIndex]
}

func (s *State) GetCommandTerm(commandIndex uint64) uint64 {
	return s.GetCommand(commandIndex).CommandTerm
}

func (s *State) GetCurrentIndex(commandIndex uint64) uint64 {
	if len(s.Log) == 0 {
		return NONEXIST
	}
	first_command_index := s.Log[0].CommandIndex
	if first_command_index > commandIndex {
		return TRUNCATED
	} else if commandIndex-first_command_index > uint64(len(s.Log)-1) {
		return NONEXIST
	}
	return commandIndex - first_command_index
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func Equal(arg1 uint64, arg2 uint64) bool {
	if arg1 == TRUNCATED || arg2 == TRUNCATED {
		return true
	}
	if arg1 == NONEXIST || arg2 == NONEXIST {
		return false
	} else {
		return arg1 == arg2
	}

}

func (s *State) CopyLog() []mpb.Command {
	l := make([]mpb.Command, len(s.Log))
	copy(l, s.Log)
	return l
}

func inSlice(s []uint64, e uint64) int {
	if s == nil {
		return -1
	}
	length := len(s)
	for i := 0; i < length; i++ {
		if (s)[i] == e {
			return i
		}
	}
	return -1
}

func rmSlice(s *MajorityQuorum, e uint64) {
	index := -1
	for i, v := range *s {
		if v == e {
			index = i
			break
		}
	}
	if index == -1 {
		return
	}
	for i := index + 1; i < len(*s); i++ {
		(*s)[i-1] = (*s)[i]
	}
	(*s) = (*s)[:len(*s)-1]
}

func (s *State) connectedPeers(contexts []mpb.Ctx) *[]uint64 {
	if len(contexts) == 0 {
		return &[]uint64{}
	}
	connections := make([]uint64, 0)
	connections = append(connections, s.Me)
	for _, k := range contexts {
		conn, exists := s.Connections[k]
		if !exists {
			continue
		}
		for _, id := range conn.Ids {
			if inSlice(connections, id) == -1 {
				connections = append(connections, id)
			}
		}
	}
	sort.Slice(connections, func(i, j int) bool { return connections[i] < connections[j] })
	return &connections
}

// setPeers sets the peer list and extends the per-peer arrays (votes, match
// indices) when a membership change adds a server, so that a server outside
// the initial cluster can be indexed. The arrays are copied, not modified in
// place, since states share them.
func (s *State) setPeers(peers []uint64) {
	if n := len(peers); n > len(s.Votes) {
		votes := make([]bool, n)
		copy(votes, s.Votes)
		s.Votes = votes
	}
	if n := len(peers); n > len(s.MmatchIndex) {
		match := make([]uint64, n)
		copy(match, s.MmatchIndex)
		s.MmatchIndex = match
	}
	s.Peers = peers
}
