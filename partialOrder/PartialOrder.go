package partialOrder

import (
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

func ExplorePaths(start state.State, pendingMessages []messagepb.Msg, end messagepb.Msg) []Path {
	//todo: first interpret the end state
	//////fmt.Printf("!!!!!!!!!!!!!START TO EXPLORE, STATE: %+v\n, PENDINGS: %v\n", start, pendingMessages)
	path := make([]*node, 0)
	path = append(path, &node{s: start})
	result := make([]Path, 0)
	e, req := Inference(end)
	////fmt.Printf("MESSAGE IS: %+v\n INFERENCED STATE IS: %+v\n PENDINGS ARE: %+v\n", end, e, pendingMessages)
	Explore(path, EndState{End: e, Req: req}, pendingMessages, &result)
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
	if end.End == nil {
		return s, true
	}
	switch end.End.(type) {
	case state.PartialState:
		return PartialStateMatched(end.End.(state.PartialState), s)
	case messagepb.Resp:
		if end.End.(messagepb.Resp).Type == messagepb.NormalResp {
			// //////fmt.Printf("")
			index := math.MaxInt64
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
