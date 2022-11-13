package partialOrder

import (
	// "fmt"
	// "fmt"

	// "fmt"

	"reflect"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/state"
	"go.etcd.io/etcd/v3/statepb"
)

//nil events
var checkLeaderEvent messagepb.Message = messagepb.Message{Type: messagepb.CheckLeader}
var checkCommitIndex messagepb.Message = messagepb.Message{Type: messagepb.CheckCommitIndex}
var timeout messagepb.Message = messagepb.Message{Type: messagepb.Timeout}

var CHECKLEADER int = -1
var CHECKCOMMITINDEX int = -2
var TIMEOUT int = -3

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
	path  []int
	state state.State
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

func pendingGC(pending []messagepb.Message, s state.State, in []int) []int {
	// //fmt.Printf("pendinggc: p: %+v, rm: %+v, in: %+v\n", p, rm_index, in)
	// debug.PrintStack()
	index := make([]int, len(in))
	copy(index, in)

	for i := 0; i < len(pending); i++ {
		if Drop(pending[i], s) {
			// if pending[i].Type != messagepb.RaftRequest && pending[i].Type != messagepb.RaftResp {
			// 	//fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!drop the message: %+v based on state: %+v\n\n\n", pending[i], s)
			// }
			for j := 0; j < len(index); j++ {
				if i == index[j] {
					index = append(index[:j], index[j+1:]...)
					break
				}
			}
			// i = i - 1
		}
	}

	return index
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

func subPath(p1 []int, p2 []int) (bool, bool) {

	if len(p1) < len(p2) {
		if len(p1) != len(p2)-1 {
			return false, false
		}
		i := 0
		for ; i < len(p1); i++ {
			if p1[i] != p2[i] {
				break
			}
		}
		for ; i < len(p1); i++ {
			if p1[i] != p2[i+1] {
				return false, false
			}
		}
		return true, false
	} else {
		if len(p2) != len(p1)-1 {
			return false, false
		}
		i := 0
		for ; i < len(p2); i++ {
			if p2[i] != p1[i] {
				break
			}
		}
		for ; i < len(p2); i++ {
			if p2[i] != p1[i+1] {
				return false, false
			}
		}
		return true, true
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

func TraverseAllMinimumPaths(start_s state.State, end_s EndState, pendings []messagepb.Message, v_index []int, p []int, paths *[]Path, counter *int) {
	// //fmt.Printf("the length of the pending message: %+v\n valid index: %+v\n", len(pendings), v_index)
	// //fmt.Printf("in search.... start: %+v\n end: %+v\n path: %+v\n valid index: %+v\n", start_s, end_s, p, v_index)
	s, matched := matched(start_s, end_s)
	*counter = *counter + 1
	if matched {
		// pendings, _ := pendingGC(pendings, s, math.MaxInt32, make([]int, 0))
		// append_check_minimum(paths, Path{state: s, path: p, pendings: pendings})
		append_check_minimum(paths, Path{path: p, state: s})
		return
	}

	for i := 0; i < len(v_index); i++ {
		index := v_index[i]
		valid_index := make([]int, len(v_index))
		copy(valid_index, v_index)

		m := pendings[index]
		msg_enabled, next, s_next := enabled(start_s, m, end_s)
		if msg_enabled {
			if !next {
				s_next, _ = Apply(start_s, m)
			}
			if reflect.DeepEqual(s_next, start_s) {
				continue
			}
			path := make([]int, len(p))
			copy(path, p)
			in := pendingGC(pendings, s_next, append(valid_index[0:i], valid_index[i+1:]...))
			TraverseAllMinimumPaths(s_next, end_s, pendings, in, append(path, index), paths, counter)
		}
	}

	//fmt.Printf("nil event traverse all Starts: start state: %+v\n", start_s)
	//nil events check:
	msg_enabled, next, s_next := enabled(start_s, checkLeaderEvent, end_s)
	//fmt.Printf("checkleader enabled: %+v\n", msg_enabled)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, checkLeaderEvent)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			path := make([]int, len(p))
			copy(path, p)
			//fmt.Printf("check leader continue... start pending gc\n")
			in := pendingGC(pendings, s_next, v_index)
			//fmt.Printf("pending gc finishes\n")
			TraverseAllMinimumPaths(s_next, end_s, pendings, in, append(path, CHECKLEADER), paths, counter)
		}
	}

	msg_enabled, next, s_next = enabled(start_s, checkCommitIndex, end_s)
	//fmt.Printf("checkcommitindex enabled: %+v\n", msg_enabled)
	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, checkCommitIndex)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			path := make([]int, len(p))
			copy(path, p)
			in := pendingGC(pendings, s_next, v_index)
			TraverseAllMinimumPaths(s_next, end_s, pendings, in, append(path, CHECKCOMMITINDEX), paths, counter)
		}
	}

	msg_enabled, next, s_next = enabled(start_s, timeout, end_s)
	//fmt.Printf("timeout enabled: %+v\n", msg_enabled)

	if msg_enabled {
		if !next {
			s_next, _ = Apply(start_s, timeout)
		}
		if !reflect.DeepEqual(s_next, start_s) {
			path := make([]int, len(p))
			copy(path, p)
			in := pendingGC(pendings, s_next, v_index)
			TraverseAllMinimumPaths(s_next, end_s, pendings, in, append(path, TIMEOUT), paths, counter)
		}
	}

}

func append_check_equivalence(result *[]Start, s state.State, pendingMessages []messagepb.Message, v_index []int) {
	remaining_pendings := make([]messagepb.Message, 0)
	for i := 0; i < len(v_index); i++ {
		remaining_pendings = append(remaining_pendings, pendingMessages[v_index[i]])
	}

	// for i := 0; i < len(*result); i++ {
	// 	if reflect.DeepEqual(Start{States: s, Remaining_pending: remaining_pendings}, (*result)[i]) {
	// 		return
	// 	}
	// }
	*result = append(*result, Start{States: s, Remaining_pending: remaining_pendings})
}

// func ExploreTest(s state.State, minimumPath []messagepb.Message, end_s EndState, pendingMessages []messagepb.Message, result *[]Start) {
// 	// current_state := CopyState(s)
// 	// for i := 0; i < len(minimumPath); i++ {
// 	// 	current_state, _ = Apply(current_state, minimumPath[i])
// 	// }
// 	// current_state, _ = matched(current_state, end_s)
// 	append_check_equivalence(result, Start{States: s, Remaining_pending: pendingMessages})
// }

// func ExploreTest(path []int, start state.State, result *[]Start, p []messagepb.Message, e EndState) {
// 	// current_state := CopyState(s)
// 	// for i := 0; i < len(minimumPath); i++ {
// 	// 	current_state, _ = Apply(current_state, minimumPath[i])
// 	// }
// 	// current_state, _ = matched(current_state, end_s)
// 	// fmt.Printf("-----------------explore test final state start--------------- state: %+v path: %+v\n", start, path)
// 	// for i := 0; i < len(path); i++ {
// 	// 	index := path[i]
// 	// 	start, _ = Apply(start, fetchMessage(p, index))
// 	// }
// 	// start, _ = matched(start, e)
// 	valid_index := make([]int, 0)
// 	for i := 0; i < len(p); i++ {
// 		found := false
// 		for j := 0; j < len(path); j++ {
// 			if i == path[j] {
// 				found = true
// 			}
// 		}
// 		if !found {
// 			valid_index = append(valid_index, i)
// 		}
// 	}

// 	in := pendingGC(p, start, valid_index)
// 	// fmt.Printf("-----------------explore test final state--------------- state: %+v path: %+v\n", start, path)
// 	// remaining_pendings := make([]messagepb.Message, 0)

// 	// for i := 0; i < len(p); i++ {
// 	// 	for j := 0; j < len(in); j++ {
// 	// 		if i == in[j] {
// 	// 			remaining_pendings = append(remaining_pendings, p[i])
// 	// 			break
// 	// 		}
// 	// 	}
// 	// }
// 	append_check_equivalence(result, start, p, in)
// }

func ExploreTest(path []int, start state.State, result *[]Start, p []messagepb.Message) {
	// current_state := CopyState(s)
	// for i := 0; i < len(minimumPath); i++ {
	// 	current_state, _ = Apply(current_state, minimumPath[i])
	// }
	// current_state, _ = matched(current_state, end_s)
	// fmt.Printf("-----------------explore test final state start--------------- state: %+v path: %+v\n", start, path)
	// for i := 0; i < len(path); i++ {
	// 	index := path[i]
	// 	start, _ = Apply(start, fetchMessage(p, index))
	// }
	// start, _ = matched(start, e)
	valid_index := make([]int, 0)
	for i := 0; i < len(p); i++ {
		found := false
		for j := 0; j < len(path); j++ {
			if i == path[j] {
				found = true
			}
		}
		if !found {
			valid_index = append(valid_index, i)
		}
	}

	in := pendingGC(p, start, valid_index)
	// fmt.Printf("-----------------explore test final state--------------- state: %+v path: %+v\n", start, path)
	// remaining_pendings := make([]messagepb.Message, 0)

	// for i := 0; i < len(p); i++ {
	// 	for j := 0; j < len(in); j++ {
	// 		if i == in[j] {
	// 			remaining_pendings = append(remaining_pendings, p[i])
	// 			break
	// 		}
	// 	}
	// }
	append_check_equivalence(result, start, p, in)
}

func Explore(s state.State, minimumPath []int, end_s EndState, pendingMessages []messagepb.Message, v_index []int, result *[]Start) {
	current_state := CopyState(s)

	if len(minimumPath) == 0 {
		final_state, matched := matched(current_state, end_s)
		if matched {
			in := pendingGC(pendingMessages, final_state, v_index)
			append_check_equivalence(result, final_state, pendingMessages, in)
		}
		return
	}

	if len(v_index) == 0 {
		s := current_state
		for i := 0; i < len(minimumPath); i++ {
			s, _ = Apply(s, fetchMessage(pendingMessages, minimumPath[i]))
		}
		final_state, matched := matched(s, end_s)
		if matched {
			in := pendingGC(pendingMessages, final_state, v_index)
			append_check_equivalence(result, final_state, pendingMessages, in)
		}
		return
	}

	//cases: minimumpath and pendingmessages are not empty
	minimumPathNextMessage := fetchMessage(pendingMessages, minimumPath[0])
	for i := 0; i < len(v_index); i++ {
		msg := pendingMessages[v_index[i]]
		if dependent(minimumPathNextMessage, msg) {
			msg_enabled, next, s_next := enabled(current_state, msg, end_s)
			if msg_enabled {
				if !next {
					s_next, _ = Apply(current_state, msg)
				}
				valid_index := make([]int, len(v_index))
				copy(valid_index, v_index)
				in := pendingGC(pendingMessages, s_next, append(valid_index[:i], valid_index[i+1:]...))
				Explore(s_next, minimumPath, end_s, pendingMessages, in, result)
			}
		}
	}

	s_next, _ := Apply(current_state, minimumPathNextMessage)
	in := pendingGC(pendingMessages, s_next, v_index)
	Explore(s_next, minimumPath[1:], end_s, pendingMessages, in, result)
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
	// fmt.Printf("-------------------------start to explore paths--------------------------------- message: %+v pending messages: %+v\n", end.Type, len(pendingMessages))
	s := make([]*node, 0)
	s = append(s, &node{s: start})
	paths := make([]Path, 0)
	path := make([]int, 0)
	e, req := Inference(end)

	fp := filterPendings(start, pendingMessages, e)
	//fmt.Printf("filtered messages: %+v\n", fp)
	end_s := EndState{End: e, Req: req, Connections: connections, Valid_indice: valid_indices}
	counter := 0
	TraverseAllMinimumPaths(start, end_s, pendingMessages, fp, path, &paths, &counter)

	// fmt.Printf("message type: %+v find the minimum paths ended: %+v counter: %+v, filtered message: %+v\n", end.Type, len(paths), counter, len(fp))
	result := make([]Start, 0)
	for i := 0; i < len(paths); i++ {
		p := paths[i].path
		remaining_pendings := make([]int, 0)
		for j := 0; j < len(pendingMessages); j++ {
			for k := 0; k < len(p); k++ {
				if j == p[k] {
					break
				}
			}
			remaining_pendings = append(remaining_pendings, j)
		}
		// Explore(start, paths[i].path, end_s, pendingMessages, remaining_pendings, &result)
		ExploreTest(p, paths[i].state, &result, pendingMessages)
	}
	//fmt.Printf("------------------------explore paths end-------------------------------")
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
