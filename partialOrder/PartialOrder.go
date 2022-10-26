package partialOrder

import (
	// "fmt"
	// "fmt"

	"math"
	"reflect"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/state"
	"go.etcd.io/etcd/v3/statepb"
)

//nil events
var checkLeaderEvent messagepb.Message = messagepb.Message{Type: messagepb.CheckLeader}
var checkCommitIndex messagepb.Message = messagepb.Message{Type: messagepb.CheckCommitIndex}
var timeout messagepb.Message = messagepb.Message{Type: messagepb.Timeout}

type Msg struct {
	msg     messagepb.Message
	visited bool
}

type node struct {
	s           state.State
	current_msg messagepb.Message
	backtrack   []*Msg
}

type Start struct {
	End_s             state.State
	Remaining_pending []messagepb.Message
}

type Path struct {
}

type EndState struct {
	End          interface{}
	Req          messagepb.Message
	Connections  []uint64
	Valid_indice []uint64
}

type MinimumStart struct {
	End_s    state.State
	messages []messagepb.Msg
}

func (n *node) AddToBacktrack(msg messagepb.Message, visited bool) {
	for i := 0; i < len(n.backtrack); i++ {
		if reflect.DeepEqual(n.backtrack[i].msg, msg) {
			return
		}
	}
	n.backtrack = append(n.backtrack, &Msg{msg: msg, visited: visited})
}

func (n *node) Visit() (bool, messagepb.Message) {
	for i := 0; i < len(n.backtrack); i++ {
		m := *(n.backtrack[i])
		if m.visited {
			continue
		} else {
			message := m.msg
			n.backtrack[i].visited = true
			return true, message
		}
	}
	return false, messagepb.Message{}
}

func dependent(msg1 messagepb.Message, msg2 messagepb.Message) bool {
	if msg1.Type == messagepb.MsgAppResp || msg2.Type == messagepb.MsgAppResp {
		return false
	}
	if msg1.Type == messagepb.MsgVoteResp && msg2.Type == messagepb.MsgVoteResp {
		return false
	}
	return true
}

func Drop(msg messagepb.Message, s state.State) bool {
	switch msg.Type {
	case messagepb.MsgApp:
		if msg.Mterm < s.CurrentTerm {
			return true
		}
		return false
	case messagepb.MsgAppResp:
		if msg.Mterm < s.CurrentTerm {
			return true
		}
		if msg.Mterm == s.CurrentTerm && msg.Success == false {
			return true
		}
		if msg.Mterm == s.CurrentTerm && msg.MatchIndex <= s.CommitIndex {
			return true
		}
		return false
	case messagepb.MsgVote:
		if msg.Mterm < s.CurrentTerm {
			return true
		}
		return false
	case messagepb.MsgVoteResp:
		if msg.Mterm < s.CurrentTerm {
			return true
		}
		if msg.Mterm == s.CurrentTerm && msg.Success == false {
			return true
		}
		if msg.Mterm == s.CurrentTerm && s.State == statepb.Leader {
			return true
		}
		return false
	default:
		return false
	}
}

func pendingGC(p []messagepb.Message, s state.State, rm_index int, in []int) ([]messagepb.Message, []int) {
	// fmt.Printf("pendinggc: p: %+v, rm: %+v, in: %+v\n", p, rm_index, in)
	// debug.PrintStack()
	pending := make([]messagepb.Message, len(p))
	copy(pending, p)
	index := make([]int, len(in))
	copy(index, in)

	for i := 0; i < len(in); i++ {
		if index[i] > rm_index {
			index[i] = index[i] - 1
		}
	}

	for i := 0; i < len(pending); i++ {
		if Drop(pending[i], s) {
			// if pending[i].Type != messagepb.RaftRequest && pending[i].Type != messagepb.RaftResp {
			// 	fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!drop the message: %+v based on state: %+v\n\n\n", pending[i], s)
			// }
			pending = append(pending[:i], pending[i+1:]...)
			for j := 0; j < len(index); j++ {
				if i == index[j] {
					index = append(index[:j], index[j+1:]...)
					j = j - 1
					continue
				} else if i < index[j] {
					index[j] = index[j] - 1
				}
			}
			i = i - 1
		}
	}

	return pending, index
}

func PickOneEnabledMsg(s state.State, end_s EndState, pendingMessages []messagepb.Message) (bool, messagepb.Message) {
	for i := 0; i < len(pendingMessages); i++ {
		// if end_s.enabled(s, pendingMessages[i]) {
		msg_enabled, _, _ := enabled(s, pendingMessages[i], end_s)
		if msg_enabled {
			return true, pendingMessages[i]
		}
	}
	//////fmt.Printf("!!!!!!!!!!!!PICK ONE MESSAGE FAILED: %+v\n", pendingMessages)
	return false, messagepb.Message{}
}

func equivalent(p1 Start, p2 Start) (bool, bool) {
	// if !StateEqual(p1.End_s, p2.End_s) {
	// 	return false, false
	// }
	p1_r := make([]messagepb.Message, len(p1.Remaining_pending))
	p2_r := make([]messagepb.Message, len(p2.Remaining_pending))
	copy(p1_r, p1.Remaining_pending)
	copy(p2_r, p2.Remaining_pending)
	if len(p1_r) < len(p2_r) {
		for i := 0; i < len(p1_r); i++ {
			found := false
			for j := 0; j < len(p2_r); j++ {
				if reflect.DeepEqual(p1_r[i], p2_r[j]) {
					found = true
					p2_r = append(p2_r[:j], p2_r[j+1:]...)
					continue
				}
			}
			if !found {
				return false, false
			}
		}
		return true, true
	} else {
		for i := 0; i < len(p2_r); i++ {
			found := false
			for j := 0; j < len(p1_r); j++ {
				if reflect.DeepEqual(p2_r[i], p1_r[j]) {
					found = true
					p1_r = append(p1_r[:j], p1_r[j+1:]...)
					continue
				}
			}
			if !found {
				return false, false
			}
		}
		return true, false
	}
}

func append_dereplicate(result *[]Start, p Start) {
	add_p := false
	exist_eq := false
	for i := 0; i < len(*result); i++ {
		eq, pos := equivalent((*result)[i], p)
		if eq {
			exist_eq = true
			if !pos {
				return
			} else {
				*result = append((*result)[:i], (*result)[i+1:]...)
				add_p = true
				i = i - 1
			}
		}
	}
	if add_p || !exist_eq {
		*result = append(*result, p)
	}
}

func TraverseAllStarts(start_s state.State, end_s EndState, position int, current_msg messagepb.Message, p []messagepb.Message, v_index []int, result *[]Start, round *int, counter *int) {
	*round = *round + 1
	s, matched := matched(start_s, end_s)
	if matched {
		pendings, _ := pendingGC(p, s, math.MaxInt32, make([]int, 0))
		append_dereplicate(result, Start{End_s: s, Remaining_pending: pendings})
		return
	}

	for i := 0; i < len(v_index); i++ {
		index := v_index[i]
		if index < position {
			if !dependent(p[index], current_msg) {
				continue
			}
		}
		pendingMessages := make([]messagepb.Message, len(p))
		copy(pendingMessages, p)

		valid_index := make([]int, len(v_index))
		copy(valid_index, v_index)

		*counter = *counter + 1

		msg_enabled, next, s_next := enabled(start_s, pendingMessages[index], end_s)
		if msg_enabled {
			if !next {
				s_next, _ = Apply(start_s, pendingMessages[index])
			}
			if reflect.DeepEqual(s_next, start_s) {
				continue
			}
			pending, in := pendingGC(append(pendingMessages[0:index], pendingMessages[index+1:]...), s_next, index, append(valid_index[0:i], valid_index[i+1:]...))
			TraverseAllStarts(s_next, end_s, i, pendingMessages[index], pending, in, result, round, counter)
		}
	}

	// fmt.Printf("nil event traverse all Starts: start state: %+v\n", start_s)
	//nil events check:
	msg_enabled, next, s_next := enabled(start_s, checkLeaderEvent, end_s)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, checkLeaderEvent)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			TraverseAllStarts(s_next, end_s, position, current_msg, p, v_index, result, round, counter)
		}
	}

	msg_enabled, next, s_next = enabled(start_s, checkCommitIndex, end_s)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, checkCommitIndex)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			TraverseAllStarts(s_next, end_s, position, current_msg, p, v_index, result, round, counter)
		}
	}

	msg_enabled, next, s_next = enabled(start_s, timeout, end_s)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, timeout)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			TraverseAllStarts(s_next, end_s, position, current_msg, p, v_index, result, round, counter)
		}
	}

}

func Explore(start []*node, end_s EndState, pendingMessages []messagepb.Message, result *[]Start) {
	last_node := start[len(start)-1]
	s, matched := matched(last_node.s, end_s)
	if matched {
		*result = append(*result, Start{End_s: s, Remaining_pending: pendingMessages})
		return
	}

	for i := 0; i < len(pendingMessages); i++ {
		pendingMessage := pendingMessages[i]
		depend_index := -1
		for j := 0; j < len(start); j++ {
			node := start[j]
			current_msg := node.current_msg
			if !dependent(current_msg, pendingMessage) {
				continue
			}
			depend_index = int(j)
		}
		if depend_index != -1 {
			node := start[depend_index]
			// if end_s.enabled(node.s, pendingMessage) {
			msg_enabled, _, _ := enabled(node.s, pendingMessage, end_s)
			if msg_enabled {
				node.AddToBacktrack(pendingMessage, false)
			}
		}
	}

	found, enabled_msg := PickOneEnabledMsg(last_node.s, end_s, pendingMessages)
	if !found {
		return
	}
	last_node.AddToBacktrack(enabled_msg, false)

	for {
		bt_found, msg := last_node.Visit()
		if bt_found {
			last_node.current_msg = msg
			new_p := make([]*node, len(start))
			copy(new_p, start)
			s_next, _ := Apply(last_node.s, msg)
			new_p = append(new_p, &node{s: s_next})
			Explore(new_p, end_s, RemoveMsg(pendingMessages, msg), result)
		} else {
			break
		}
	}
}

func filterPendings(start state.State, pendingMessages []messagepb.Message, end interface{}) []int {
	switch end.(type) {
	case state.PartialState:
		appResp := false
		voteResp := false
		clientReq := false
		if start.CurrentTerm != end.(state.PartialState).CurrentTerm {
			voteResp = true
			appResp = true
		}
		if start.State != end.(state.PartialState).State && end.(state.PartialState).State == statepb.Leader {
			voteResp = true
		}
		if start.CommitIndex < end.(state.PartialState).CommitIndex {
			appResp = true
		}
		end_length := len(end.(state.PartialState).Log)
		if start.GetLastCommandIndex() < end.(state.PartialState).Log[end_length-1].CommandIndex {
			clientReq = true
		}
		res := make([]int, 0)
		for i := 0; i < len(pendingMessages); i++ {
			switch pendingMessages[i].Type {
			case messagepb.MsgAppResp:
				if appResp {
					res = append(res, i)
				}
			case messagepb.MsgVoteResp:
				if voteResp {
					res = append(res, i)
				}
			case messagepb.ClientNormal, messagepb.ClientKVPut, messagepb.ClientKVRO, messagepb.ClientConfChange:
				if clientReq {
					res = append(res, i)
				}
			}
		}
		return res

	default:
		res := make([]int, len(pendingMessages))
		for i := 0; i < len(pendingMessages); i++ {
			res[i] = i
		}
		return res
	}
}

func ExploreStarts(start state.State, pendingMessages []messagepb.Message, end messagepb.Message, connections []uint64, valid_indices []uint64) []Start {
	// fmt.Printf("explore Start from state: %+v\n to state: %+v\n ", start, end)
	Start := make([]*node, 0)
	Start = append(Start, &node{s: start})
	result := make([]Start, 0)
	e, req := Inference(end)

	round := 0
	counter := 0
	fp := filterPendings(start, pendingMessages, e)
	TraverseAllStarts(start, EndState{End: e, Req: req, Connections: connections, Valid_indice: valid_indices}, 0, messagepb.Message{}, pendingMessages, fp, &result, &round, &counter)
	return result
}

func RemoveMsg(pendingMessages []messagepb.Message, msg messagepb.Message) []messagepb.Message {
	for i := 0; i < len(pendingMessages); i++ {
		if reflect.DeepEqual(pendingMessages[i], msg) {
			pendingMessages = append(pendingMessages[:i], pendingMessages[i+1:]...)
			return pendingMessages
		}
	}
	return pendingMessages
}

func matched(s state.State, end EndState) (state.State, bool) {
	if end.End == nil {
		return s, true
	}
	switch end.End.(type) {
	case state.PartialState:
		return PartialStateMatched(end.End.(state.PartialState), s)
	case messagepb.Message:
		if end.End.(messagepb.Message).Type == messagepb.ClientNormalResp {
			index := 0
			for i := 0; i < len(s.Log); i++ {
				if reflect.DeepEqual(s.Log[i].Command.ReqID, end.End.(messagepb.Message).ReqID) {
					index = i
					break
				}
			}
			return s, s.GetCommitIndex() >= (uint64(index) + s.GetFirstCommandIndex())
		} else if end.End.(messagepb.Message).Type == messagepb.ClientROResp {
			return s, (s.QuorumActive(end.Connections) || s.IndexValid(end.Valid_indice)) && s.SearchGet(end.End.(messagepb.Message))
		}
	default:
		s, r := Apply(s, end.Req)
		return s, reflect.DeepEqual(r, end.End)
	}
	return s, true
}

func enabled(s state.State, msg messagepb.Message, end EndState) (bool, bool, state.State) {
	s = CopyState(s)
	switch end.End.(type) {
	case state.PartialState:
		return ParitialStateEnabled(end.End.(state.PartialState), s, msg)
	case messagepb.Message:
		end_msg := end.End.(messagepb.Message)
		if msg.Mterm > end_msg.Mterm {
			return false, false, state.State{}
		}
		switch end_msg.Type {
		case messagepb.ClientNormalResp, messagepb.ClientROResp:
			return true, false, state.State{}
		case messagepb.MsgAppResp:
			s, _ := Apply(s, msg)
			if end_msg.Success {
				return LogMatch(s, end.Req.MEntries, s.CommitIndex, false), true, s
			}
			return true, true, s
		case messagepb.MsgVoteResp:
			s, _ := Apply(s, msg)
			if end_msg.Success {
				if s.CurrentTerm == end_msg.Mterm && s.VotedFor != end_msg.To && s.VotedFor != state.NOONE {
					return false, true, s
				}
			}
			return true, true, s
		}
	}
	return true, false, state.State{}
}
