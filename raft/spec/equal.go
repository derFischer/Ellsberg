package spec

func JointQuorumEqual(q1 JointQuorum, q2 JointQuorum) bool {
	if &(q1[0]) != &(q2[0]) {
		if !MajorityQuorumEqual(q1[0], q2[0]) || !MajorityQuorumEqual(q1[1], q2[1]) {
			return false
		}
	}
	return true
}

func MajorityQuorumEqual(q1 MajorityQuorum, q2 MajorityQuorum) bool {
	if len(q1) != len(q2) {
		return false
	}
	if len(q1) == 0 {
		return true
	}
	if &(q1[0]) != &(q2[0]) {
		for _, v := range q1 {
			if inSlice(q2, v) == -1 {
				return false
			}
		}
	}
	return true
}
