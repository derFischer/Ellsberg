package spec

import (
	"log"

	mpb "go.nyu.edu/protosim/raft/messagepb"
)

func (s *State) Truncate(all_valid_context *map[mpb.Ctx]int) {
	s.LogGC()
	new_map := make(map[mpb.Ctx]Connection)

	for k, v := range s.Connections {
		c, exist := (*all_valid_context)[k]
		if !exist || c == 0 {
			continue
		}
		new_map[k] = v
	}
	s.Connections = new_map
}

func (s *State) LogGC() {
	if len(s.Log) <= 31 {
		return
	}
	local_index := s.GetCurrentIndex(s.CommitIndex)
	if local_index == TRUNCATED {
		log.Fatalf("The last committed entry has been truncated, incorrect! commit index: %+v, first index: %+v\n", s.CommitIndex, s.GetFirstCommandIndex())
	}
	if local_index == NONEXIST {
		log.Fatal("The commit index is larger than the end of the log!\n")
	}
	if s.CommitIndex < s.GetLastCommandIndex()-30 {
		s.Log = s.Log[local_index:]
	} else {
		s.Log = s.Log[s.GetCurrentIndex(s.GetLastCommandIndex()-30):]
	}
}
