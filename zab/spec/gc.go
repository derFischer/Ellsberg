package spec

func (s *State) GC() {
	//truncate the log to the committed index
	if len(s.Log) <= 30 {
		return
	}
	index := s.ZxidToIndex(s.Log, s.LastCommitted)
	if len(s.Log)-index > 30 {
		s.Log = s.Log[index:]
	} else {
		s.Log = s.Log[len(s.Log)-30:]
	}
}
