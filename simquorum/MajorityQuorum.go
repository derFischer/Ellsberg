package simquorum

import (
	"math"
	"sort"
)

type MajorityQuorum []uint64

func findMajority(array []uint64) uint64 {
	cp := make([]uint64, len(array))
	copy(cp, array)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i] < cp[j]
	})
	length := len(cp)
	return cp[(length-1)/2]
}

func (m *MajorityQuorum) CommitIndex(AckedIndex *[]uint64, Peers *[]uint64) uint64 {
	if len(*m) == 0 {
		return math.MaxUint64
	}
	result := []uint64{}
	for idx, peer := range *Peers {
		ok := (inSlice(*m, peer)) != -1
		if !ok {
			continue
		}
		result = append(result, (*AckedIndex)[idx])
	}

	for i := len(result); i < len(*m); i++ {
		result = append(result, 0)
	}

	return findMajority(result)
}

func (m *MajorityQuorum) VoteSuccess(AckedVote *[]bool, Peers *[]uint64) bool {
	if len(*m) == 0 {
		return true
	}
	votes := 0
	for idx, peer := range *Peers {
		ok := (inSlice(*m, peer)) != -1
		if !ok {
			continue
		}
		if (*AckedVote)[idx] {
			votes = votes + 1
		}
	}
	return votes >= (len(*m)/2 + 1)
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
