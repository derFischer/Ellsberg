package simquorum

type JointQuorum [2]MajorityQuorum

func (j *JointQuorum) CommitIndex(AckedIndex *[]uint64, Peers *[]uint64) uint64 {
	idx1 := j[0].CommitIndex(AckedIndex, Peers)
	idx2 := j[1].CommitIndex(AckedIndex, Peers)
	if idx1 > idx2 {
		return idx2
	} else {
		return idx1
	}
}

func (j *JointQuorum) VoteSuccess(AckedVote *[]bool, Peers *[]uint64) bool {
	return j[0].VoteSuccess(AckedVote, Peers) && j[1].VoteSuccess(AckedVote, Peers)
}
