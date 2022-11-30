package partialOrder

import (
	// "fmt"
	// "fmt"

	// "fmt"

	// "fmt"
	// "fmt"
	"fmt"
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

type Pendings struct {
	pendingMessages []int
	explored        []bool
}

type Start struct {
	States            *state.State
	Remaining_pending []messagepb.Message
}

//helper
type Start_helper struct {
	States            *state.State
	Remaining_pending []int
}

type Visited struct {
	s        *state.State
	pendings []Pendings
	pos      int
	reached  bool
	finished bool
}

type EndState struct {
	End          interface{}
	Req          messagepb.Message
	Connections  []uint64
	Valid_indice []uint64
}

func (v *Visited) addPendings(idx []Pendings) {
	for _, p := range idx {
		exists := false
		add := true
		for i := 0; i < len(v.pendings); i++ {
			vp := v.pendings[i]
			included, pos := int_slice_included(&vp.pendingMessages, &p.pendingMessages)
			if included {
				exists = true
			}
			if pos == 2 {
				add = false
				break
			}
			if pos == 1 {
				if i == len(v.pendings)-1 {
					v.pendings = v.pendings[:i]
				} else {
					v.pendings = append(v.pendings[:i], v.pendings[i+1:]...)
				}
			}
		}
		if !exists || add {
			explored := make([]bool, len(p.pendingMessages))
			for i, _ := range explored {
				explored[i] = false
			}
			v.pendings = append(v.pendings, Pendings{pendingMessages: p.pendingMessages, explored: explored})
		}
		// v.pendings = append(v.pendings, Pendings{pendingMessages: p.pendingMessages})
	}
	// fmt.Printf("after add pendings, the node: %+v\n", *v)
}

func (v *Visited) fetchOne(s *state.State, pendingmessages *[]messagepb.Message) (bool, int, []Pendings) {
	// fmt.Printf("enter fetch one: the remaining msg in the node: %+v\n", v)
	if len(v.pendings) == 0 {
		return false, -1, nil
	}
	pendings := v.pendings[v.pos].pendingMessages
	idx := 0
	for i, e := range v.pendings[v.pos].explored {
		if e == false {
			idx = i
			break
		}
	}
	result := pendings[idx]
	result_p := make([]Pendings, 0)

	//mark this message as explored from all pending lists
	for i := 0; i < len(v.pendings); i++ {
		pm := v.pendings[i].pendingMessages
		pe := v.pendings[i].explored
		for j, idx := range pm {
			if idx == result {
				pe[j] = true

				remaining_p := make([]int, len(pm))
				copy(remaining_p, pm)
				if j == len(remaining_p)-1 {
					remaining_p = remaining_p[:j]
				} else {
					remaining_p = append(remaining_p[:j], remaining_p[j+1:]...)
				}
				result_p = append(result_p, Pendings{pendingMessages: remaining_p})

				if j == len(pm)-1 {
					if i == len(v.pendings)-1 {
						v.pendings = v.pendings[:i]
					} else {
						v.pendings = append(v.pendings[:i], v.pendings[i+1:]...)
					}
				}
				break
			}
		}
	}
	if len(v.pendings) != 0 {
		v.pos = (v.pos + 1) % (len(v.pendings))
	}
	// fmt.Printf("exit fetch one: the remaining msg in the node: %+v\n", v)
	return true, result, result_p
}

func (v *Visited) allPendingGC(s *state.State, pendingmessages *[]messagepb.Message) {
	for i := 0; i < len(v.pendings); i++ {
		pm := pendingGC(pendingmessages, s, &v.pendings[i].pendingMessages)
		if len(pm) != 0 {
			pe := make([]bool, len(pm))
			for i, _ := range pe {
				pe[i] = false
			}
			v.pendings[i] = Pendings{pendingMessages: pm, explored: pe}
		} else {
			if i == len(v.pendings)-1 {
				v.pendings = v.pendings[0:i]
			} else {
				v.pendings = append(v.pendings[0:i], v.pendings[i+1:]...)
			}
			i = i - 1
		}
	}
	if len(v.pendings) != 0 {
		v.pos = (v.pos + 1) % (len(v.pendings))
	}
}

func Drop(msg messagepb.Message, s *state.State) bool {
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

func pendingGC(pending *[]messagepb.Message, s *state.State, in *[]int) []int {
	// //fmt.Printf("pendinggc: p: %+v, rm: %+v, in: %+v\n", p, rm_index, in)
	// debug.PrintStack()
	index := make([]int, len(*in))
	copy(index, *in)

	for i := 0; i < len(index); i++ {
		if Drop(fetchMessage(pending, index[i]), s) {
			if i == len(index)-1 {
				index = index[:i]
			} else {
				index = append(index[:i], index[i+1:]...)
				i = i - 1
			}
		}
	}

	return index
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

func ExploreFromNode(start *state.State, n int, visited *[](*Visited), pendingMessages *[]messagepb.Message, end_s *EndState, counter *int) bool {

	// fmt.Printf("the current visited nodes:\n")
	// for _, n := range *visited {
	// 	fmt.Printf("%+v\n", *n)
	// }
	// fmt.Printf("---------------------------------")
	node := (*visited)[n]
	if node.finished {
		return false
	}

	s, reached := matched(*start, end_s)
	if reached {
		node.s = &s
		node.reached = true
		node.finished = true
		return false
	}

	for {
		conti, idx, rpd := node.fetchOne(start, pendingMessages)
		if !conti {
			*visited = append((*visited)[0:n], (*visited)[n+1:]...)
			node.finished = true
			return true
		}
		// m := pendingMessages[idx]
		m := fetchMessage(pendingMessages, idx)
		*counter = *counter + 1
		msg_enabled, next, s := enabled(*start, m, end_s)
		if msg_enabled {
			if !next {
				s, _ = Apply(*start, m)
			}
		} else {
			continue
		}
		// fmt.Printf("in this step, message idx: %+v, before state: %+v \n after state: %+v\n", idx, start, s)
		// node.allPendingGC(&s, pendingMessages)
		found := false
		for idx, nd := range *visited {
			if s.Equal(nd.s) {
				found = true
				if idx == n {
					break
				}
				// fmt.Printf("the state is the same: %+v\n", nd.s)
				nd.addPendings(rpd)
				break
			}
		}
		if !found {
			new_node := Visited{s: &s, pendings: []Pendings{}, pos: 0}
			new_node.addPendings(rpd)
			new_node.addPendings([]Pendings{Pendings{pendingMessages: []int{CHECKCOMMITINDEX, CHECKLEADER, TIMEOUT}}})
			new_node.allPendingGC(&s, pendingMessages)
			// fmt.Printf("add new node! %+v\n", new_node)
			*visited = append((*visited), &new_node)
			break
		}
	}
	return false
}

func int_slice_included(s1 *[]int, s2 *[]int) (bool, int) {
	r1 := make([]int, len(*s1))
	r2 := make([]int, len(*s2))
	copy(r1, *s1)
	copy(r2, *s1)
	if len(r1) < len(r2) {
		for _, v1 := range r1 {
			found := false
			for i := 0; i < len(r2); i++ {
				v2 := r2[i]
				if v2 == v1 {
					found = true

					if i == len(r2)-1 {
						r2 = r2[:i]
					} else {
						r2 = append(r2[:i], r2[i+1:]...)
					}
					break
				}
			}
			if !found {
				return false, -1
			}
		}
		return true, 1
	} else {
		for _, v2 := range r2 {
			found := false
			for i := 0; i < len(r1); i++ {
				v1 := r1[i]
				if v2 == v1 {
					found = true

					if i == len(r1)-1 {
						r1 = r1[:i]
					} else {
						r1 = append(r1[:i], r1[i+1:]...)
					}
					break
				}
			}
			if !found {
				return false, -1
			}
		}
		return true, 2
	}
}

func path_included(s1 *Start_helper, s2 *Start_helper) (bool, int) {
	if !s1.States.Equal(s2.States) {
		return false, -1
	}
	return int_slice_included(&s1.Remaining_pending, &s2.Remaining_pending)
}

func append_check_equivalence(s1 *Start_helper, result *[]Start_helper) {
	included := false
	add_p := false
	for i := 0; i < len(*result); i++ {
		in, pos := path_included(s1, &((*result)[i]))
		if in && pos == 2 {
			return
		}
		if in {
			if i == len(*result)-1 {
				*result = (*result)[:i]
			} else {
				*result = append((*result)[:i], (*result)[i+1:]...)
			}
			i = i - 1
			included = true
			add_p = true
		}
	}
	if !included || add_p {
		*result = append(*result, *s1)
	}
}

func convert_to_start(result *[]Start_helper, pendingmessages *[]messagepb.Message) []Start {
	r := make([]Start, 0)
	for _, sh := range *result {
		pm := make([]messagepb.Message, 0)
		for _, idx := range sh.Remaining_pending {
			if idx == CHECKCOMMITINDEX || idx == CHECKLEADER || idx == TIMEOUT {
				continue
			}
			pm = append(pm, (*pendingmessages)[idx])
		}
		r = append(r, Start{States: sh.States, Remaining_pending: pm})
	}
	return r
}

func ExploreBFS(start *state.State, end_s *EndState, pendingMessages *[]messagepb.Message) []Start {
	// fmt.Printf("------------------------pending messages:-------------------------\n")
	// for _, m := range *pendingMessages {
	// 	printMessage(m)
	// }
	counter := 0

	pendings_index := make([]int, len((*pendingMessages)))
	pendings_explored := make([]bool, len((*pendingMessages)))
	for i, _ := range pendings_index {
		pendings_index[i] = i
		pendings_explored[i] = false
	}
	pendings_index = append(pendings_index, CHECKCOMMITINDEX)
	pendings_index = append(pendings_index, CHECKLEADER)
	pendings_index = append(pendings_index, TIMEOUT)
	pendings_explored = append(pendings_explored, false)
	pendings_explored = append(pendings_explored, false)
	pendings_explored = append(pendings_explored, false)
	visited := []*Visited{&Visited{s: start, pendings: []Pendings{Pendings{pendingMessages: pendings_index, explored: pendings_explored}}, reached: false, finished: false, pos: 0}}
	for {
		finished := true
		for i := 0; i < len(visited); i++ {
			node := visited[i]
			del := ExploreFromNode(node.s, i, &visited, pendingMessages, end_s, &counter)
			if node.finished == false {
				finished = false
			}
			if del {
				i = i - 1
			}
		}
		if finished {
			break
		}
		// fmt.Printf("test??????????finished: %+v\n", finished)
	}

	// fmt.Printf("check here!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

	result := make([]Start_helper, 0)
	for _, node := range visited {
		if node.reached {
			for _, p := range node.pendings {
				append_check_equivalence(&Start_helper{States: node.s, Remaining_pending: p.pendingMessages}, &result)
			}
		}
	}
	fmt.Printf("pending message length: %+v, explored messages: %+v\n", len(*pendingMessages), counter)

	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!this exploration: pending messages: %+v\n, result: %+v\n", pendingMessages, result)

	return convert_to_start(&result, pendingMessages)
}

func ExplorePaths(start *state.State, pendingMessages *[]messagepb.Message, end messagepb.Message, connections []uint64, valid_indices []uint64) []Start {
	e, req := Inference(end)
	end_s := EndState{End: e, Req: req, Connections: connections, Valid_indice: valid_indices}

	return ExploreBFS(start, &end_s, pendingMessages)
}

func printMessage(m messagepb.Message) {
	if m.Type == messagepb.MsgAppResp {
		fmt.Printf("Type: MsgAppResp, From: %+v, Term: %+v, Accept: %+v, MatchIndex: %+v\n", m.From, m.Mterm, m.Success, m.MatchIndex)
		return
	}
	if m.Type == messagepb.MsgVoteResp {
		fmt.Printf("Type: MsgVoteResp, From: %+v, Term: %+v, Accept: %+v\n", m.From, m.Mterm, m.Success)
		return
	}
	if m.Type == messagepb.CheckLeader {
		fmt.Printf("Type: checkLeader\n")
		return
	}
	if m.Type == messagepb.CheckCommitIndex {
		fmt.Printf("Type: checkCommitIndex\n")
		return
	}
	if m.Type == messagepb.Timeout {
		fmt.Printf("Type: timeout\n")
		return
	}
	if m.Type == messagepb.ClientNormal || m.Type == messagepb.ClientKVPut {
		fmt.Printf("Type: clientReq, ID: %+v, DATA: %+v\n", m.ReqID, m.Data)
		return
	}
	fmt.Printf("UNKNOWN MESSAGE! %+v\n", m)
}

func printPendings(fp []int, messages *[]messagepb.Message) {
	fmt.Printf("filtered messages are: \n")
	for i := 0; i < len(fp); i++ {
		printMessage(fetchMessage(messages, fp[i]))
	}
}

func matched(s state.State, end *EndState) (state.State, bool) {
	if end.End == nil {
		return s, true
	}
	switch end.End.(type) {
	case state.PartialState:
		return PartialStateMatched(&(end.End), s)
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
			// return s, true
		} else if end.End.(messagepb.Message).Type == messagepb.ClientROResp {
			return s, (s.QuorumActive(end.Connections) || s.IndexValid(end.Valid_indice)) && s.SearchGet(end.End.(messagepb.Message))
		}
	default:
		s, _ := Apply(s, end.Req)
		// return s, reflect.DeepEqual(r, end.End)
		return s, true
	}
	return s, true
}

// bool, bool, state.State:
// bool: whether the msg is enabled based on the current state and the end state
// bool: whether the next state after applying this msg is available (to avoid the cost of applying the message again in the future)
// state: the next state after applying msg, if available. Otherwise, return State{}
func enabled(s state.State, msg messagepb.Message, end *EndState) (bool, bool, state.State) {
	// s = CopyState(s)
	switch end.End.(type) {
	case state.PartialState:
		return ParitialStateEnabled(&(end.End), s, msg)
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
