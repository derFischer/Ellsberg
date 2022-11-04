package partialOrder

import (
	// "fmt"
	// "fmt"

	"fmt"
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
	state   state.State
	visited bool
}

type node struct {
	s state.State
	// current_msg messagepb.Message
	// next_set    []*Msg
}

type Start struct {
	States            state.State
	Remaining_pending []messagepb.Message
}

type Path struct {
	state    state.State
	path     []messagepb.Message
	pendings []messagepb.Message
	test     []int
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

// func (n *node) AddToNextSet(msg messagepb.Message, visited bool, state state.State) {
// 	for i := 0; i < len(n.next_set); i++ {
// 		if reflect.DeepEqual(n.next_set[i].msg, msg) {
// 			return
// 		}
// 	}
// 	n.next_set = append(n.next_set, &Msg{msg: msg, visited: visited, state: state})
// }

// func (n *node) Visit() (bool, messagepb.Message) {
// 	for i := 0; i < len(n.next_set); i++ {
// 		m := *(n.next_set[i])
// 		if m.visited {
// 			continue
// 		} else {
// 			message := m.msg
// 			n.next_set[i].visited = true
// 			return true, message
// 		}
// 	}
// 	return false, messagepb.Message{}
// }

func dependent(msg1 messagepb.Message, msg2 messagepb.Message) bool {
	if msg1.Type == messagepb.MsgAppResp || msg2.Type == messagepb.MsgAppResp {
		return false
	}
	if msg1.Type == messagepb.MsgVoteResp && msg2.Type == messagepb.MsgVoteResp {
		return false
	}
	if msg1.Type == messagepb.CheckCommitIndex || msg1.Type == messagepb.Timeout || msg1.Type == messagepb.CheckLeader {
		return false
	}
	if msg2.Type == messagepb.CheckCommitIndex || msg2.Type == messagepb.Timeout || msg2.Type == messagepb.CheckLeader {
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
		msg_enabled, _, _ := enabled(s, pendingMessages[i], end_s)
		if msg_enabled {
			return true, pendingMessages[i]
		}
	}
	return false, messagepb.Message{}
}

func subPath(p1 []messagepb.Message, p2 []messagepb.Message) (bool, bool) {

	if len(p1) < len(p2) {
		j := 0
		for i := 0; i < len(p1); i++ {
			found := false
			for ; j < len(p2); j++ {
				if reflect.DeepEqual(p1[i], p2[j]) {
					found = true
					continue
				}
			}
			if !found {
				return false, true
			}
		}
		return true, true
	} else {
		j := 0
		for i := 0; i < len(p2); i++ {
			found := false
			for ; j < len(p1); j++ {
				if reflect.DeepEqual(p2[i], p1[j]) {
					found = true
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

func append_check_minimum(paths *[]Path, p Path) {
	add_p := false
	exist_eq := false
	for i := 0; i < len(*paths); i++ {
		sub, pos := subPath((*paths)[i].path, p.path)
		if sub {
			exist_eq = true
			if !pos {
				return
			} else {
				*paths = append((*paths)[:i], (*paths)[i+1:]...)
				add_p = true
				i = i - 1
			}
		}
	}
	if add_p || !exist_eq {
		*paths = append(*paths, p)
	}
	// *paths = append(*paths, p)
}

func TraverseAllMinimumPaths(start_s state.State, end_s EndState, position int, current_msg messagepb.Message, pendings []messagepb.Message, v_index []int, p []messagepb.Message, paths *[]Path, round *int, counter *int, p_t []int) {
	// fmt.Printf("the length of the pending message: %+v\n valid index: %+v\n", len(pendings), v_index)
	*round = *round + 1
	s, matched := matched(start_s, end_s)
	if matched {
		pendings, _ := pendingGC(pendings, s, math.MaxInt32, make([]int, 0))
		// append_check_minimum(paths, Path{state: s, path: p, pendings: pendings})
		append_check_minimum(paths, Path{state: s, path: p, pendings: pendings, test: p_t})
		return
	}

	for i := 0; i < len(v_index); i++ {
		index := v_index[i]
		if index < position {
			if !dependent(pendings[index], current_msg) {
				continue
			}
		}
		pendingMessages := make([]messagepb.Message, len(pendings))
		copy(pendingMessages, pendings)

		valid_index := make([]int, len(v_index))
		copy(valid_index, v_index)

		*counter = *counter + 1

		m := pendingMessages[index]
		msg_enabled, next, s_next := enabled(start_s, m, end_s)
		if msg_enabled {
			if !next {
				s_next, _ = Apply(start_s, m)
			}
			if reflect.DeepEqual(s_next, start_s) {
				continue
			}
			path := make([]messagepb.Message, len(p))
			copy(path, p)
			pending, in := pendingGC(append(pendingMessages[0:index], pendingMessages[index+1:]...), s_next, index, append(valid_index[0:i], valid_index[i+1:]...))
			p_t_t := make([]int, len(p_t))
			copy(p_t_t, p_t)
			TraverseAllMinimumPaths(s_next, end_s, i, pendingMessages[index], pending, in, append(path, m), paths, round, counter, append(p_t_t, index))
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
			pendingMessages := make([]messagepb.Message, len(pendings))
			copy(pendingMessages, pendings)
			valid_index := make([]int, len(v_index))
			copy(valid_index, v_index)
			path := make([]messagepb.Message, len(p))
			copy(path, p)
			pendings, in := pendingGC(pendingMessages, s_next, math.MaxInt32, valid_index)
			p_t_t := make([]int, len(p_t))
			copy(p_t_t, p_t)
			TraverseAllMinimumPaths(s_next, end_s, position, current_msg, pendings, in, append(path, checkLeaderEvent), paths, round, counter, append(p_t_t, -1))
		}
	}

	msg_enabled, next, s_next = enabled(start_s, checkCommitIndex, end_s)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, checkCommitIndex)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			pendingMessages := make([]messagepb.Message, len(pendings))
			copy(pendingMessages, pendings)
			valid_index := make([]int, len(v_index))
			copy(valid_index, v_index)
			path := make([]messagepb.Message, len(p))
			copy(path, p)
			pendings, in := pendingGC(pendingMessages, s_next, math.MaxInt32, valid_index)
			p_t_t := make([]int, len(p_t))
			copy(p_t_t, p_t)
			TraverseAllMinimumPaths(s_next, end_s, position, current_msg, pendings, in, append(path, checkCommitIndex), paths, round, counter, append(p_t_t, -2))
		}
	}

	msg_enabled, next, s_next = enabled(start_s, timeout, end_s)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, timeout)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			pendingMessages := make([]messagepb.Message, len(pendings))
			copy(pendingMessages, pendings)
			valid_index := make([]int, len(v_index))
			copy(valid_index, v_index)
			path := make([]messagepb.Message, len(p))
			copy(path, p)
			p_t_t := make([]int, len(p_t))
			copy(p_t_t, p_t)
			pendings, in := pendingGC(pendingMessages, s_next, math.MaxInt32, valid_index)
			TraverseAllMinimumPaths(s_next, end_s, position, current_msg, pendings, in, append(path, timeout), paths, round, counter, append(p_t_t, -3))
		}
	}

}

func append_check_equivalence(result *[]Start, s Start) {
	for i := 0; i < len(*result); i++ {
		if reflect.DeepEqual((*result)[i], s) {
			return
		}
	}
	*result = append(*result, s)
}

// func ExploreTest(s state.State, minimumPath []messagepb.Message, end_s EndState, pendingMessages []messagepb.Message, result *[]Start) {
// 	// current_state := CopyState(s)
// 	// for i := 0; i < len(minimumPath); i++ {
// 	// 	current_state, _ = Apply(current_state, minimumPath[i])
// 	// }
// 	// current_state, _ = matched(current_state, end_s)
// 	append_check_equivalence(result, Start{States: s, Remaining_pending: pendingMessages})
// }

func ExploreTest(path Path, result *[]Start) {
	// current_state := CopyState(s)
	// for i := 0; i < len(minimumPath); i++ {
	// 	current_state, _ = Apply(current_state, minimumPath[i])
	// }
	// current_state, _ = matched(current_state, end_s)
	append_check_equivalence(result, Start{States: path.state, Remaining_pending: path.pendings})
}

func Explore(s state.State, minimumPath []messagepb.Message, end_s EndState, pendingMessages []messagepb.Message, result *[]Start) {
	current_state := CopyState(s)

	if len(minimumPath) == 0 {
		s, matched := matched(current_state, end_s)
		if matched {
			append_check_equivalence(result, Start{States: s, Remaining_pending: pendingMessages})
		}
		return
	}

	if len(pendingMessages) == 0 {
		s := current_state
		for i := 0; i < len(minimumPath); i++ {
			s, _ = Apply(s, minimumPath[i])
		}
		final_state, matched := matched(s, end_s)
		if matched {
			append_check_equivalence(result, Start{States: final_state, Remaining_pending: pendingMessages})
		}
		return
	}

	//cases: minimumpath and pendingmessages are not empty
	minimumPathNextMessage := minimumPath[0]
	for i := 0; i < len(pendingMessages); i++ {
		msg := pendingMessages[i]
		if dependent(minimumPathNextMessage, msg) {
			msg_enabled, next, s_next := enabled(current_state, msg, end_s)
			if msg_enabled {
				if !next {
					s_next, _ = Apply(current_state, msg)
				}
				new_p := make([]messagepb.Message, len(pendingMessages))
				copy(new_p, pendingMessages)
				Explore(s_next, minimumPath, end_s, append(new_p[:i], new_p[i+1:]...), result)
			}
		}
	}

	s_next, _ := Apply(current_state, minimumPath[0])
	Explore(s_next, minimumPath[1:], end_s, pendingMessages, result)
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
			voteResp = true
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

func ExplorePaths(start state.State, pendingMessages []messagepb.Message, end messagepb.Message, connections []uint64, valid_indices []uint64) []Start {
	// fmt.Printf("explore Start from state: %+v\n to state: %+v, pendings: %+v\n ", start, end, pendingMessages)
	s := make([]*node, 0)
	s = append(s, &node{s: start})
	paths := make([]Path, 0)
	path := make([]messagepb.Message, 0)
	p_t := make([]int, 0)
	e, req := Inference(end)

	round := 0
	counter := 0
	fp := filterPendings(start, pendingMessages, e)
	// fmt.Printf("the length of pending messgages: %+v\n fp: %+v\n", len(pendingMessages), fp)
	end_s := EndState{End: e, Req: req, Connections: connections, Valid_indice: valid_indices}
	TraverseAllMinimumPaths(start, end_s, 0, messagepb.Message{}, pendingMessages, fp, path, &paths, &round, &counter, p_t)
	// fmt.Printf("minimum paths found for the transition: %+v\n", paths)
	fmt.Printf("find the minimum paths ended: %+v.\n", len(paths))
	result := make([]Start, 0)
	for i := 0; i < len(paths); i++ {
		// Explore(start, paths[i].path, end_s, paths[i].pendings, &result)
		// ExploreTest(start, paths[i].path, end_s, paths[i].pendings, &result)
		ExploreTest(paths[i], &result)
	}
	// fmt.Printf("explore from the minimum path ended.\n")
	// fmt.Printf("paths found for the transition: %+v\n", len(result))

	return result
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

// bool, bool, state.State:
// bool: whether the msg is enabled based on the current state and the end state
// bool: whether the next state after applying this msg is available (to avoid the cost of applying the message again in the future)
// state: the next state after applying msg, if available. Otherwise, return State{}
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
