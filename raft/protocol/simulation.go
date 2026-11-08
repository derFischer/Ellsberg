package protocol

import (
	"fmt"
	"log"
	"reflect"

	"go.nyu.edu/protosim/engine"
	"go.nyu.edu/protosim/network"
	"go.nyu.edu/protosim/raft/messagepb"
	"go.nyu.edu/protosim/raft/spec"
	"go.nyu.edu/protosim/statistics"
)

// ROEntry is a read-only client request waiting for its reply, with the
// ReadIndex contexts the server sent while it was waiting.
type ROEntry struct {
	ReqID          interface{}
	ConnectionCtxs []messagepb.Ctx
}

// Simulation checks the trace of one Raft server.
type Simulation struct {
	me          uint64
	checker     *engine.Checker[spec.State, messagepb.Message, Pending, EndState, ClientBuffer]
	raft        *Raft
	Connections map[messagepb.Ctx]int // ReadIndex contexts, with the number of reads still using them
	ROReq       []ROEntry
	app         string
	out         messagepb.Message // the outgoing message being checked
}

// StartSimulationServer returns the checker of server id of a cluster of peers.
func StartSimulationServer(id uint64, peers []uint64, app string) *Simulation {
	client_req_buffer := make(map[interface{}]uint64)
	return newSimulation(id, app, &Candidate{S: spec.InitialState(id, peers), X: &client_req_buffer})
}

func newSimulation(id uint64, app string, initial *Candidate) *Simulation {
	sl := &Simulation{me: id, app: app, Connections: make(map[messagepb.Ctx]int), ROReq: make([]ROEntry, 0)}
	sl.raft = &Raft{valid: &sl.Connections}
	sl.checker = &engine.Checker[spec.State, messagepb.Message, Pending, EndState, ClientBuffer]{
		Proto:      sl.raft,
		Net:        network.UnorderedNet[messagepb.Message]{},
		Candidates: []*Candidate{initial},
	}
	return sl
}

func (s *Simulation) SendMsg(msg messagepb.Message, next_msg *messagepb.Message) (success bool) {
	// A response records the request it answers: the request was received.
	if msg.Type == messagepb.MsgVoteResp || msg.Type == messagepb.MsgAppResp {
		var req_msg messagepb.Message
		if msg.Type == messagepb.MsgAppResp {
			req_msg = messagepb.Message{Type: messagepb.MsgApp, From: msg.To, To: msg.From, Mterm: msg.ReqTerm, Index: msg.Index, IndexTerm: msg.IndexTerm, MEntries: msg.MEntries, CommitIndex: msg.CommitIndex}
		} else {
			req_msg = messagepb.Message{Type: messagepb.MsgVote, From: msg.To, To: msg.From, Mterm: msg.ReqTerm, Index: msg.Index, IndexTerm: msg.IndexTerm}
		}
		s.checker.Receive(&req_msg)
	}
	s.InitializeCtx(msg.Ctx)
	statistics.Stats.RecordSendMsgType(int8(msg.Type))
	contexts, gcctx := s.ROSendPreprocess(msg)
	s.raft.contexts = contexts
	s.out = msg
	if !s.checker.Send(&s.out, next_msg) {
		s.dump()
		log.Printf("No valid states for this message: %+v\n", msg)
		return false
	}
	s.ROSendPostprocess(gcctx)
	return true
}

func (s *Simulation) ReceiveMsg(msg messagepb.Message) {
	s.ROReceivePreprocess(msg)
	s.checker.Receive(&msg)

	max_pendings_count := 0
	for _, c := range s.checker.Candidates {
		if n := c.P.Distinct(); n > max_pendings_count {
			max_pendings_count = n
		}
	}
	statistics.Stats.RecordRemainingStates(uint64(len(s.checker.Candidates)))
	statistics.Stats.RecordPendingMsgCount(uint64(max_pendings_count))
}

func (s *Simulation) ROSendPreprocess(msg messagepb.Message) ([]messagepb.Ctx, []messagepb.Ctx) {
	var contexts []messagepb.Ctx
	CtxForGC := make([]messagepb.Ctx, 0)
	if msg.Type == messagepb.ClientROResp {
		found, entry := s.searchInROReq(msg.ReqID)
		if !found {
			log.Fatalf("haven't received the client req for response %+v before\n", msg)
		}
		contexts = entry.ConnectionCtxs
		for _, c := range contexts {
			ref, exists := s.Connections[c]
			if !exists {
				log.Fatalf("context %+v gced before the client request hasn't been finished\n", c)
			}
			s.Connections[c] = ref - 1
			if ref == 1 {
				CtxForGC = append(CtxForGC, c)
			}
		}
	}
	return contexts, CtxForGC
}

func (s *Simulation) ROSendPostprocess(CtxForGC []messagepb.Ctx) {
	for _, c := range CtxForGC {
		delete(s.Connections, c)
	}
}

func (s *Simulation) ROReceivePreprocess(msg messagepb.Message) {
	if msg.Type == messagepb.ClientKVRO {
		s.ROReq = append(s.ROReq, ROEntry{ReqID: msg.ReqID})
	}
}

func (s *Simulation) InitializeCtx(ctx messagepb.Ctx) {
	if ctx == nil || len(s.ROReq) == 0 {
		return
	}

	found := s.searchConnection(ctx, true, len(s.ROReq))
	if !found {
		for i := 0; i < len(s.ROReq); i++ {
			s.ROReq[i].ConnectionCtxs = append(s.ROReq[i].ConnectionCtxs, ctx)
		}
	}
}

func (s *Simulation) searchConnection(ctx messagepb.Ctx, add bool, ref int) bool {
	_, exists := s.Connections[ctx]
	if exists {
		return true
	}
	if add {
		s.Connections[ctx] = ref
	}
	return false
}

func (s *Simulation) searchInROReq(ID interface{}) (bool, ROEntry) {
	length := len(s.ROReq)
	for i := 0; i < length; i++ {
		if reflect.DeepEqual(s.ROReq[i].ReqID, ID) {
			entry := s.ROReq[i]
			s.ROReq = append(s.ROReq[:i], s.ROReq[i+1:]...)
			return true, entry
		}
	}
	return false, ROEntry{}
}

func (s *Simulation) Dump() {
	s.dump()
}

func (s *Simulation) dump() {
	fmt.Printf("-----------------------------------------------states size is %+v--------------------------------------------------\n\n", len(s.checker.Candidates))
	fmt.Printf("the valid connections are: %+v\n", s.Connections)
	for _, c := range s.checker.Candidates {
		dumpCandidate(c)
	}
}

func dumpPending(p Pending) {
	fmt.Printf("remaining message:\n\n")
	for i := 0; i < p.Slots(); i++ {
		fmt.Printf("number: %+v, ", p.Count(i))
		PrintMessage(*p.Next(i))
	}
	fmt.Printf("\n\n\n\n")
}
