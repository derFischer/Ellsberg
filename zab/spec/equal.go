package spec

import "reflect"

func (s *State) Equal(st *State) bool {
	if s.ZabState != st.ZabState {
		return false
	}
	if !(s.ElectionStates == st.ElectionStates) {
		if s.ElectionStates.State != st.ElectionStates.State {
			return false
		}
		if s.ElectionStates.ConnectInfo.Sid != st.ElectionStates.ConnectInfo.Sid || s.ElectionStates.ConnectInfo.NlRcv != st.ElectionStates.ConnectInfo.NlRcv {
			return false
		}
		if !arrayEqual(s.ElectionStates.Learners, st.ElectionStates.Learners, len(s.Peers)) {
			return false
		}

		//states only used in DISCOVERY PHASE
		if s.ElectionStates.tempMaxEpoch != st.ElectionStates.tempMaxEpoch {
			return false
		}
		if !arrayEqual(s.ElectionStates.Connecting, st.ElectionStates.Connecting, len(s.Peers)) {
			return false
		}
		//states only used in SYNCHRONIZATION PHASE
		if !arrayEqual(s.ElectionStates.AckLdRecv, st.ElectionStates.AckLdRecv, len(s.Peers)) {
			return false
		}
		if !reflect.DeepEqual(s.ElectionStates.Election, st.ElectionStates.Election) {
			return false
		}
	}
	if s.ZabState != st.ZabState || s.LastCommitted != st.LastCommitted || s.AcceptEpoch != st.AcceptEpoch || s.Epoch != st.Epoch || s.LastProcessed != st.LastProcessed {
		return false
	}
	if &(s.Log[0]) != &(st.Log[0]) {
		if s.GetLastZxid() != st.GetLastZxid() {
			return false
		}
		i := 0
		j := 0
		for {
			if i >= len(s.Log) || j >= len(st.Log) {
				break
			}
			entry_s := s.Log[i]
			entry_st := st.Log[j]
			if entry_s.CommandIndex != entry_st.CommandIndex || entry_s.CommandTerm != entry_st.CommandTerm || entry_s.Command.ReqID != entry_st.Command.ReqID {
				return false
			}
			if !arrayEqual(&entry_s.AckId, &entry_st.AckId, len(s.Peers)) {
				return false
			}
			i = i + 1
			j = j + 1
		}
	}

	// if s.PacketsSync != st.PacketsSync {
	// 	if s.PacketsSync == nil || st.PacketsSync == nil {
	// 		if !(s.PacketsSync == nil && st.PacketsSync == nil) {
	// 			return false
	// 		}
	// 	}
	// 	if len(s.PacketsSync.Uncommitted) != len(st.PacketsSync.Uncommitted) {
	// 		return false
	// 	}
	// 	for i, v := range s.PacketsSync.Uncommitted {
	// 		if v != st.PacketsSync.Uncommitted[i] {
	// 			return false
	// 		}
	// 	}
	// }
	return true
}

func arrayEqual(arr1 *[]uint64, arr2 *[]uint64, max_length int) bool {
	if arr1 == arr2 {
		return true
	}
	//return true if all elements in arr1 also in arr2, and all elements in arr2 also in arr1
	if len(*arr1) != len(*arr2) {
		return false
	}
	if len(*arr1) == max_length {
		return true
	}
	for _, v := range *arr1 {
		if inSlice(*arr2, v) == -1 {
			return false
		}
	}
	return true
}

// func ackEpochEqual(ack1 map[uint64]AckEpochInfo, ack2 map[uint64]AckEpochInfo, inSync bool) bool {
// 	if inSync {
// 		return reflect.DeepEqual(ack1, ack2)
// 	}
// 	//return true if all elements in ack1 also in ack2, and all elements in ack2 also in ack1, ignore the value of inQuorum
// 	if len(ack1) != len(ack2) {
// 		return false
// 	}
// 	for k, v := range ack1 {
// 		v2, ok := (ack2)[k]
// 		if !ok {
// 			return false
// 		}
// 		if v.LastProcessed != v2.LastProcessed {
// 			return false
// 		}
// 	}
// 	return true
// }
