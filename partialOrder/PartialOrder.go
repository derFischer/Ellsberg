package partialOrder

import (
	// "fmt"

	"log"
	"math"
	"reflect"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/raft/raftpb"
	"go.etcd.io/etcd/v3/state"
	"go.etcd.io/etcd/v3/statepb"
)

type Msg struct {
	msg     messagepb.Msg
	visited bool
}

type node struct {
	s           state.State
	current_msg messagepb.Msg
	backtrack   []*Msg
}

type Path struct {
	End_s             state.State
	Remaining_pending []messagepb.Msg
}

type EndState struct {
	End interface{}
	Req interface{}
}

type MinimumPath struct {
	End_s    state.State
	messages []messagepb.Msg
}

func (n *node) AddToBacktrack(msg messagepb.Msg, visited bool) {
	for i := 0; i < len(n.backtrack); i++ {
		if n.backtrack[i].msg == msg {
			return
		}
	}
	n.backtrack = append(n.backtrack, &Msg{msg: msg, visited: visited})
}

func (n *node) Visit() (bool, messagepb.Msg) {
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
	return false, messagepb.Msg{}
}

func dependent(msg1 messagepb.Msg, msg2 messagepb.Msg) bool {
	if msg1.Msg == nil || msg2.Msg == nil {
		return false
	}
	t1, _, _ := messagepb.MsgInterpreter(msg1)
	t2, _, _ := messagepb.MsgInterpreter(msg2)
	if t1 == messagepb.MsgAppResp || t2 == messagepb.MsgAppResp || t1 == messagepb.MsgVoteResp || t2 == messagepb.MsgVoteResp {
		return false
	}
	return true
}

func Drop(msg messagepb.Msg, s state.State) bool {
	t, m, _ := messagepb.MsgInterpreter(msg)
	switch t {
	case messagepb.MsgApp:
		if m.(messagepb.AppendEntriesArgs).MTerm < s.CurrentTerm {
			return true
		}
		return false
	case messagepb.MsgAppResp:
		if m.(messagepb.AppendEntriesReply).MTerm > s.CurrentTerm {
			return false
		}
		if m.(messagepb.AppendEntriesReply).MSuccess == false {
			return true
		}
		if m.(messagepb.AppendEntriesReply).MTerm < s.CurrentTerm || m.(messagepb.AppendEntriesReply).MMatchIndex <= s.CommitIndex {
			return true
		}
		return false
	case messagepb.MsgVote:
		if m.(messagepb.RequestVoteArgs).MTerm < s.CurrentTerm {
			return true
		}
		return false
	case messagepb.MsgVoteResp:
		if m.(messagepb.RequestVoteReply).MTerm < s.CurrentTerm {
			return true
		}
		if m.(messagepb.RequestVoteReply).MVoteGranted == false {
			return true
		}
		if m.(messagepb.RequestVoteReply).MTerm == s.CurrentTerm && s.State == statepb.Leader {
			return true
		}
		return false
	case messagepb.ClientRequest:
		return false
	}
	return false
}

func pendingGC(p []messagepb.Msg, s state.State, rm_index int, in []int) ([]messagepb.Msg, []int) {
	// fmt.Printf("pendinggc: p: %+v, rm: %+v, in: %+v\n", p, rm_index, in)
	// debug.PrintStack()
	pending := make([]messagepb.Msg, len(p))
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

func PickOneEnabledMsg(s state.State, end_s EndState, pendingMessages []messagepb.Msg) (bool, messagepb.Msg) {
	for i := 0; i < len(pendingMessages); i++ {
		// if end_s.enabled(s, pendingMessages[i]) {
		if enabled(s, pendingMessages[i], end_s) {
			return true, pendingMessages[i]
		}
	}
	//////fmt.Printf("!!!!!!!!!!!!PICK ONE MESSAGE FAILED: %+v\n", pendingMessages)
	return false, messagepb.Msg{}
}

func equivalent(p1 Path, p2 Path) (bool, bool) {
	// if !StateEqual(p1.End_s, p2.End_s) {
	// 	return false, false
	// }
	p1_r := make([]messagepb.Msg, len(p1.Remaining_pending))
	p2_r := make([]messagepb.Msg, len(p2.Remaining_pending))
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

func append_dereplicate(result *[]Path, p Path) {
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

func TraverseAllPaths(start_s state.State, end_s EndState, position int, current_msg messagepb.Msg, p []messagepb.Msg, v_index []int, result *[]Path, counter *int) {
	*counter = *counter + 1
	// fmt.Printf("enter traverse paths\n\n\n")
	// fmt.Printf("the current result set is: %+v\n\n\n", *result)
	s, matched := matched(start_s, end_s)
	if matched {
		// fmt.Printf("!!!!!!!!!!!!!!!!!explore matched, state: %+v\n\n, end: %+v\n\n\n", start_s, end_s)
		// fmt.Printf("matched a trace, pendings: %+v, end state: %+v\n", pendingMessages, end_s)
		// *result = append(*result, Path{End_s: s, Remaining_pending: pendingMessages})
		pendings, _ := pendingGC(p, s, math.MaxInt32, make([]int, 0))
		// fmt.Printf("matched state log len: %+v, commit index: %+v\n", len(s.Log), s.CommitIndex)
		append_dereplicate(result, Path{End_s: s, Remaining_pending: pendings})
		// fmt.Printf("!!!!!!!!!!!!!!!!!find the trace, the result set: %+v\n\n\n", *result)
		return
	}

	for i := 0; i < len(v_index); i++ {
		index := v_index[i]
		if index < position {
			if !dependent(p[index], current_msg) {
				continue
			}
		}
		pendingMessages := make([]messagepb.Msg, len(p))
		copy(pendingMessages, p)

		valid_index := make([]int, len(v_index))
		copy(valid_index, v_index)

		if enabled(start_s, pendingMessages[index], end_s) {
			// fmt.Printf("start state: %+v\n\n\n end state: %+v\n\n\n before apply %+v, the current result set is: %+v\n\n\n", start_s, end_s, pendingMessages[i], *result)
			s_next, _ := Apply(start_s, pendingMessages[index])
			if len(s_next.Log) > 110 {
				log.Fatalf("log length > 100, commit index: %+v, last index: %+v\n", s_next.CommitIndex, s_next.GetLastCommandIndex())
			}
			// fmt.Printf("after apply %+v, the current result set is: %+v\n\n\n the state is: %+v\n\n\n", pendingMessages[i], *result, s)
			// fmt.Printf("before gc: p: %+v, index: %+v, valid_index: %+v", pendingMessages, index, valid_index)
			pending, in := pendingGC(append(pendingMessages[0:index], pendingMessages[index+1:]...), s_next, index, append(valid_index[0:i], valid_index[i+1:]...))
			TraverseAllPaths(s_next, end_s, i, pendingMessages[index], pending, in, result, counter)
		}
	}
}

// func FindMinimumSet(start_s state.State, end_s EndState, position int, current_msg messagepb.Msg, p []messagepb.Msg, msgs []messagepb.Msg, result *[]Path) {
// 	fmt.Printf("enter find minimum set\n\n\n")
// 	// fmt.Printf("the current result set is: %+v\n\n\n", *result)
// 	s, matched := matched(start_s, end_s)
// 	if matched {
// 		// fmt.Printf("!!!!!!!!!!!!!!!!!explore matched, state: %+v\n\n, end: %+v\n\n\n", start_s, end_s)
// 		// fmt.Printf("matched a trace, pendings: %+v, end state: %+v\n", pendingMessages, end_s)
// 		// *result = append(*result, Path{End_s: s, Remaining_pending: pendingMessages})
// 		append_dereplicate(result, Path{End_s: s, Remaining_pending: msgs})
// 		// fmt.Printf("!!!!!!!!!!!!!!!!!find the trace, the result set: %+v\n\n\n", *result)
// 		return
// 	}

// 	for i := 0; i < len(p); i++ {
// 		if i < position {
// 			if !dependent(p[i], current_msg) {
// 				continue
// 			}
// 		}
// 		pendingMessages := make([]messagepb.Msg, len(p))
// 		copy(pendingMessages, p)
// 		if enabled(start_s, pendingMessages[i], end_s) {
// 			// fmt.Printf("start state: %+v\n\n\n end state: %+v\n\n\n before apply %+v, the current result set is: %+v\n\n\n", start_s, end_s, pendingMessages[i], *result)
// 			s, _ := Apply(start_s, pendingMessages[i])
// 			// fmt.Printf("after apply %+v, the current result set is: %+v\n\n\n the state is: %+v\n\n\n", pendingMessages[i], *result, s)
// 			TraverseAllPaths(s, end_s, i, pendingMessages[i], pendingGC(append(pendingMessages[0:i], pendingMessages[i+1:]...), s), result)
// 		}
// 	}
// }

func Explore(path []*node, end_s EndState, pendingMessages []messagepb.Msg, result *[]Path) {
	last_node := path[len(path)-1]
	s, matched := matched(last_node.s, end_s)
	if matched {
		*result = append(*result, Path{End_s: s, Remaining_pending: pendingMessages})
		return
	}

	for i := 0; i < len(pendingMessages); i++ {
		pendingMessage := pendingMessages[i]
		depend_index := -1
		for j := 0; j < len(path); j++ {
			node := path[j]
			current_msg := node.current_msg
			if !dependent(current_msg, pendingMessage) {
				continue
			}
			depend_index = int(j)
		}
		if depend_index != -1 {
			node := path[depend_index]
			// if end_s.enabled(node.s, pendingMessage) {
			if enabled(node.s, pendingMessage, end_s) {
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
			new_p := make([]*node, len(path))
			copy(new_p, path)
			s_next, _ := Apply(last_node.s, msg)
			new_p = append(new_p, &node{s: s_next})
			Explore(new_p, end_s, RemoveMsg(pendingMessages, msg), result)
		} else {
			break
		}
	}
}

// func FindMinimumSet()

func filterPendings(start state.State, pendingMessages []messagepb.Msg, end interface{}) []int {
	switch end.(type) {
	case state.PartialState:
		appResp := false
		voteResp := false
		clientReq := false
		if start.State != end.(state.PartialState).State && end.(state.PartialState).State == statepb.Leader {
			voteResp = true
		}
		if start.CommitIndex < end.(state.PartialState).CommitIndex {
			appResp = true
		}
		if !LogMatch(start, end.(state.PartialState).Log, math.MaxUint64, true) {
			clientReq = true
		}
		res := make([]int, 0)
		for i := 0; i < len(pendingMessages); i++ {
			t, _, _ := messagepb.MsgInterpreter(pendingMessages[i])
			switch t {
			case messagepb.MsgAppResp:
				if appResp {
					res = append(res, i)
				}
			case messagepb.MsgVoteResp:
				if voteResp {
					res = append(res, i)
				}
			case messagepb.ClientRequest:
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

func ExplorePaths(start state.State, pendingMessages []messagepb.Msg, end messagepb.Msg) []Path {
	//todo: first interpret the end state
	// fmt.Printf("!!!!!!!!!!!!!START TO EXPLORE, STATE: %+v\n, PENDINGS: %v\n", start, pendingMessages)
	path := make([]*node, 0)
	path = append(path, &node{s: start})
	result := make([]Path, 0)
	e, req := Inference(end)
	// fmt.Printf("!!!!!!!!!!!The end state is: %+v\n\n\n", e)
	////fmt.Printf("MESSAGE IS: %+v\n INFERENCED STATE IS: %+v\n PENDINGS ARE: %+v\n", end, e, pendingMessages)
	// Explore(path, EndState{End: e, Req: req}, pendingMessages, &result)

	counter := 0
	TraverseAllPaths(start, EndState{End: e, Req: req}, 0, messagepb.Msg{}, pendingMessages, filterPendings(start, pendingMessages, e), &result, &counter)
	// fmt.Printf("explore finished, counter: %+v pending: %+v\n", counter, len(pendingMessages))
	return result
}

func RemoveMsg(pendingMessages []messagepb.Msg, msg messagepb.Msg) []messagepb.Msg {
	for i := 0; i < len(pendingMessages); i++ {
		if reflect.DeepEqual(pendingMessages[i], msg) {
			pendingMessages = append(pendingMessages[:i], pendingMessages[i+1:]...)
			return pendingMessages
		}
	}
	return pendingMessages
}

func matched(s state.State, end EndState) (state.State, bool) {
	// s, r := s.Apply(next_msg)
	// s := CopyState(st)
	if end.End == nil {
		return s, true
	}
	switch end.End.(type) {
	case state.PartialState:
		// _, s_1 := PartialStateMatched(end.End.(state.PartialState), s)
		// if s_1 {
		// 	fmt.Printf("partial state matched, state: %+v\n\n end: %+v\n\n\n", s, end)
		// }
		return PartialStateMatched(end.End.(state.PartialState), s)
	case messagepb.Resp:
		if end.End.(messagepb.Resp).Type == messagepb.NormalResp {
			// //////fmt.Printf("")
			index := 0
			// for i := end.(messagepb.Resp).Index; i <= s.GetLastCommandIndex(); i++ {
			for i := 0; i < len(s.Log); i++ {
				// if i < s.GetFirstCommandIndex() {
				// 	continue
				// }
				// data := s.Log[i].Command.Data
				// req := new(etcdserverpb.InternalRaftRequest)
				// req.Unmarshal(data)
				if reflect.DeepEqual(s.Log[i].Command.ReqID, end.End.(messagepb.Resp).ID) {
					index = i
					break
				}
			}
			return s, s.GetCommitIndex() >= uint64(index)
		}
	default:
		s, r := Apply(s, messagepb.Msg{Send: false, Type: messagepb.RaftRequest, Msg: messagepb.RaftMsg{Msg: end.Req.(raftpb.Message)}})
		return s, reflect.DeepEqual(r, end.End)
	}
	return s, true
}

func enabled(s state.State, msg messagepb.Msg, end EndState) bool {
	switch end.End.(type) {
	case state.PartialState:
		return ParitialStateEnabled(end.End.(state.PartialState), s, msg)
	case messagepb.Resp:
		return true
	case messagepb.AppendEntriesReply:
		s, _ := Apply(s, msg)
		if s.CurrentTerm > end.End.(messagepb.AppendEntriesReply).MTerm {
			return false
		}
		_, m, _ := messagepb.MsgInterpreter(messagepb.Msg{Send: false, Type: messagepb.RaftRequest, Msg: messagepb.RaftMsg{Msg: end.Req.(raftpb.Message)}})
		if end.End.(messagepb.AppendEntriesReply).MSuccess {
			return LogMatch(s, m.(messagepb.AppendEntriesArgs).MEntries, s.CommitIndex, false)
		}
		return true
	case messagepb.RequestVoteReply:
		s, _ := Apply(s, msg)
		if s.CurrentTerm > end.End.(messagepb.RequestVoteReply).MTerm {
			return false
		}
		_, m, _ := messagepb.MsgInterpreter(messagepb.Msg{Send: false, Type: messagepb.RaftRequest, Msg: messagepb.RaftMsg{Msg: end.Req.(raftpb.Message)}})
		if end.End.(messagepb.RequestVoteReply).MVoteGranted {
			if s.CurrentTerm == end.End.(messagepb.RequestVoteReply).MTerm && s.VotedFor != m.(messagepb.RequestVoteArgs).From && s.VotedFor != statepb.Unknown {
				return false
			}
		}
		return true
	}
	return true
}
