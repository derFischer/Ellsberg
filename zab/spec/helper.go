package spec

import "reflect"

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

func (s *State) InQuorum(set []uint64) bool {
	return len(set)*2 > len(s.Peers)
}

func (s *State) ZxidToIndex(log []*Command, zxid uint64) int {
	for i, v := range log {
		if v.CommandIndex == zxid {
			return i
		}
	}
	return -1
}

func IsNextZxid(curZxid uint64, nextZxid uint64) bool {
	curindex := curZxid & 0xffffffff
	curepoch := curZxid >> 32
	nextindex := nextZxid & 0xffffffff
	nextepoch := nextZxid >> 32

	return (nextindex == 1 && curepoch < nextepoch) || (nextindex > 1 && curepoch == nextepoch && curindex+1 == nextindex)
}

func (s *State) GetReqIndex(ri ReqIndex) uint64 {
	for _, v := range s.Log {
		if reflect.DeepEqual(v.Command.ReqID, ri) {
			return v.CommandIndex
		}
	}
	return 0
}
