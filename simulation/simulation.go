package simulation

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"sync"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/partialOrder"
	"go.etcd.io/etcd/v3/simquorum"
	"go.etcd.io/etcd/v3/state"
	"go.etcd.io/etcd/v3/statepb"
)

// import "bytes"
// import "labgob"

//
// A Go object implementing a single Raft peer.
//

type Connection struct {
	ref int
	ids []uint64
	// index uint64
}

type ROEntry struct {
	ReqID interface{}
	Ctxs  []messagepb.Ctx
}

type RIndex struct {
	ref   int
	index uint64
	// term  uint64
}

type Simulation struct {
	mu          sync.Mutex // Lock to protect shared access to this peer's state
	me          uint64
	States      []*partialOrder.Start
	MsgCh       chan messagepb.Msg
	Connections map[messagepb.Ctx]Connection
	ROReq       []ROEntry
	ReadIndex   map[messagepb.Ctx]RIndex
	msg_counter int
	req_counter int
	counter     int
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	matchIndex := make([]uint64, total)
	alive := make(map[messagepb.Ctx]Connection)
	rindex := make(map[messagepb.Ctx]RIndex)
	var q simquorum.JointQuorum
	q[0] = make([]uint64, len(peers))
	copy(q[0], peers)
	q[1] = []uint64{}

	for i := 0; i < total; i++ {
		votes[i] = false
		matchIndex[i] = 0
	}

	//hack the initial state
	lg := make([]statepb.Command, 4)
	lg[0] = statepb.Command{CommandIndex: 0}
	lg[1] = statepb.Command{CommandIndex: 1}
	lg[2] = statepb.Command{CommandIndex: 2}
	lg[3] = statepb.Command{CommandIndex: 3, CommandTerm: 1}

	sl := new(Simulation)
	sl.me = id
	sl.States = make([]*partialOrder.Start, 1)
	sl.States[0] = &partialOrder.Start{States: &state.State{Me: id, State: statepb.Follower, CurrentTerm: 1, VotedFor: state.NOONE, Votes: votes, Log: lg, CommitIndex: 3, MmatchIndex: matchIndex, Peers: peers, Quorum: q}}
	sl.MsgCh = make(chan messagepb.Msg)
	sl.Connections = alive
	sl.ReadIndex = rindex

	sl.ROReq = make([]ROEntry, 0)
	sl.counter = 0
	sl.msg_counter = 0
	sl.req_counter = 0

	go func() {
		for {
			select {
			case msg := <-sl.MsgCh:
				// if msg.Msg == nil {
				// 	log.Fatalf("This message is nil: %+v\n", msg)
				// }
				if msg.Send {
					// fmt.Printf("trying to send message: %+v\n", msg)
					sl.sendMsg(msg)
					// fmt.Printf("send message check finished\n")
				} else {
					//fmt.Printf("!!!!!!!!!!!!!!!!receive the msg: %+v\n", msg)
					// fmt.Printf("receive the message: %+v\n", msg)
					sl.receiveMsg(msg)
					// fmt.Printf("add the message finished\n")
				}
			}
		}
	}()
	return sl
}

func (s *Simulation) sendMsg(message messagepb.Msg) {
	// fmt.Printf("sending out message: %+v\n\n\n", message)
	msg, ctx := messagepb.MsgInterpreter(message)
	connected_peers, valid_indice := s.ROSendPreprocess(msg, ctx)
	s.CheckMsg(msg, connected_peers, valid_indice)
	// fmt.Printf("the states after sending the message: %+v, message type: %+v\n", len(s.States), msg.Type)
}

func (s *Simulation) receiveMsg(message messagepb.Msg) {
	// if message.Type == messagepb.ClientRequest {
	// 	if s.req_counter == 1000 {
	// 		log.Fatalf("the number of states per message: %+v\n", s.counter/s.msg_counter)
	// 	}
	// 	s.req_counter++
	// }
	msg, ctx := messagepb.MsgInterpreter(message)
	// if msg.Type != messagepb.MsgAppResp {
	// 	fmt.Printf("receive read index resp: %+v\n", msg)
	// }
	s.ROReceivePreprocess(msg, ctx)
	s.AddPendings(msg)
}

func (s *Simulation) CheckMsg(msg messagepb.Message, connected_peers []uint64, valid_indice []uint64) {
	// s.dump()
	tmp_state := make([]*partialOrder.Start, 0)
	s.msg_counter++
	for i := 0; i < len(s.States); i++ {
		Starts := partialOrder.ExplorePaths(s.States[i].States, &s.States[i].Remaining_pending, msg, connected_peers, valid_indice)
		for _, sh := range *Starts {
			s.counter++
			append_check_equivalence(sh, &tmp_state)
		}
	}
	if len(tmp_state) == 0 {
		s.dump()
		// fmt.Printf("No valid states for this message. The message: %+v\n ", msg)
		// s.dump()
		log.Fatalf("No valid states for this message: %+v\n", msg)
	}
	// fmt.Printf("the number of states before merge: %+v, after merge: %+v\n", counter, len(tmp_state))
	// if s.msg_counter
	s.States = tmp_state
}

func (s *Simulation) AddPendings(msg messagepb.Message) {
	//msg read index wouldn't affect the states
	if msg.Type == messagepb.MsgReadIndex {
		return
	}
	for i := 0; i < len(s.States); i++ {
		if partialOrder.Drop(msg, s.States[i].States) {
			continue
		}
		// t, m, _ := messagepb.MsgInterpreter(msg)
		// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!add %+v %v to pendings\n\n\n", t, m)
		s.States[i].Remaining_pending = append(s.States[i].Remaining_pending, &msg)
	}
}

//read only requests related, these requests wouldn't have any effects on the states

func (s *Simulation) ROSendPreprocess(msg messagepb.Message, ctx messagepb.Ctx) ([]uint64, []uint64) {
	s.InitializeCtx(msg.Type == messagepb.MsgReadIndex, ctx)
	connected_peers := []uint64{}
	valid_indice := []uint64{}
	// ROResp := (msg.Type == messagepb.ClientROResp)

	if msg.Type == messagepb.ClientROResp {
		found, entry := s.searchInROReq(msg.ReqID)
		if !found {
			log.Fatalf("haven't received the client req for response %+v before\n", msg)
		}
		for i := 0; i < len(entry.Ctxs); i++ {
			ctx := entry.Ctxs[i]
			connection, exists := s.Connections[ctx]
			if exists {
				for _, id := range connection.ids {
					if inSlice(connected_peers, id) == -1 {
						connected_peers = append(connected_peers, id)
					}
				}
			} else {
				valid_index, exists := s.ReadIndex[ctx]
				if !exists {
					log.Fatalf("the context %+v is neither in connections nor read index, ERROR!\n", ctx)
				}
				valid_indice = append(valid_indice, valid_index.index)
			}
		}
		// fmt.Printf("connected peers after this read: %+v\n", connected_peers)
	}
	return connected_peers, valid_indice
}

func (s *Simulation) ROReceivePreprocess(msg messagepb.Message, ctx messagepb.Ctx) {
	s.CtxAlive(msg.Type == messagepb.MsgReadIndexResp, msg.From, msg.Index, ctx)
	if msg.Type == messagepb.ClientKVRO || msg.Type == messagepb.MsgReadIndex {
		s.ROReq = append(s.ROReq, ROEntry{ReqID: msg.ReqID})
	}
}

func (s *Simulation) InitializeCtx(rindex bool, ctx messagepb.Ctx) {
	if ctx == nil {
		return
	}
	found := true
	// fmt.Printf("initialize ctx: %+v, current pending ro reqs: %+v\n", ctx, s.ROReq)
	if !rindex {
		found = s.searchConnection(ctx, true, len(s.ROReq))
	} else {
		found = s.searchRIndex(ctx, true, len(s.ROReq))
	}
	if found {
		return
	}
	for i := 0; i < len(s.ROReq); i++ {
		entry := s.ROReq[i]
		s.ROReq[i] = ROEntry{ReqID: entry.ReqID, Ctxs: append(entry.Ctxs, ctx)}
	}
}

func (s *Simulation) CtxAlive(rindex bool, id uint64, index uint64, ctx messagepb.Ctx) {
	if ctx == nil {
		return
	}
	// fmt.Printf("connection alive: %+v\n", ctx)
	found := false

	if !rindex {
		found = s.searchConnection(ctx, false, 0)
	} else {
		found = s.searchRIndex(ctx, false, 0)
	}

	if !found {
		return
	}

	if found {
		if !rindex {
			connection := s.Connections[ctx]
			if inSlice(connection.ids, id) == -1 {
				s.Connections[ctx] = Connection{ref: connection.ref, ids: append(connection.ids, id)}
			}
		} else {
			rindex := s.ReadIndex[ctx]
			s.ReadIndex[ctx] = RIndex{ref: rindex.ref, index: index}
		}
	}
}

func (s *Simulation) ConnectionGC() {
	if len(s.Connections) < 50 {
		return
	}

	for ctx, connection := range s.Connections {
		if connection.ref == 0 {
			delete(s.Connections, ctx)
		}
		if connection.ref < 0 {
			log.Fatalf("connection ref < 0! ERROR! ctx: %+v\n", ctx)
		}
	}
}

func pointer_slice_included(s1 *[]*messagepb.Message, s2 *[]*messagepb.Message) int {
	r1 := make([]*messagepb.Message, len(*s1))
	r2 := make([]*messagepb.Message, len(*s2))
	copy(r1, *s1)
	copy(r2, *s2)
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
				return -1
			}
		}
		return 2
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
				return -1
			}
		}
		return 1
	}
}

func path_included(s1 *partialOrder.Start, s2 *partialOrder.Start) int {
	if !s1.States.Equal(s2.States) {
		return -1
	}

	return pointer_slice_included(&s1.Remaining_pending, &s2.Remaining_pending)
}

// func path_included(s1 *partialOrder.Start, s2 *partialOrder.Start) int {
// 	state_equal := s1.States.EqualInclude(s2.States)
// 	if state_equal == -1 {
// 		return -1
// 	}
// 	path_equal := pointer_slice_included(&s1.Remaining_pending, &s2.Remaining_pending)
// 	if path_equal == -1 {
// 		return -1
// 	}

// 	if state_equal == 3 {
// 		return path_equal
// 	}
// 	if state_equal != path_equal {
// 		return -1
// 	}
// 	return path_equal
// }

func append_check_equivalence(s1 *partialOrder.Start, result *[]*partialOrder.Start) {
	for i := 0; i < len(*result); i++ {
		pos := path_included(s1, ((*result)[i]))
		if pos == 2 {
			return
		}
		if pos == 1 {
			if i == len(*result)-1 {
				*result = (*result)[:i]
			} else {
				*result = append((*result)[:i], (*result)[i+1:]...)
			}
			i = i - 1
		}
	}
	// fmt.Printf("new state! %+v\n", *(s1.States))
	*result = append(*result, s1)
}

func convert_to_start(result *[]partialOrder.Start_helper, pendingmessages *[]*messagepb.Message) *[]partialOrder.Start {
	r := make([]partialOrder.Start, 0)
	for _, sh := range *result {
		pm := make([]*messagepb.Message, 0)
		for _, idx := range sh.Remaining_pending {
			if idx == partialOrder.CHECKCOMMITINDEX || idx == partialOrder.CHECKLEADER || idx == partialOrder.TIMEOUT {
				continue
			}
			pm = append(pm, (*pendingmessages)[idx])
		}
		r = append(r, partialOrder.Start{States: sh.States, Remaining_pending: pm})
	}
	return &r
}

// helper function
func (s *Simulation) searchConnection(ctx messagepb.Ctx, add bool, ref int) bool {
	_, exists := s.Connections[ctx]
	if exists {
		return true
	}
	if add {
		s.Connections[ctx] = Connection{ref: ref}
	}
	return false
}

func (s *Simulation) searchRIndex(ctx messagepb.Ctx, add bool, ref int) bool {
	_, exists := s.ReadIndex[ctx]
	if exists {
		return true
	}
	if add {
		s.ReadIndex[ctx] = RIndex{ref: ref}
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

// func (s *Simulation) ActivePeers(ctxs []messagepb.Ctx)  {
// 	connected_peers := []uint64{}
// 	for i := 0; i < len(ctxs); i++ {
// 		ctx := ctxs[i]
// 		connection := s.Connections[ctx]
// 		for _, id := range connection.ids {
// 			if inSlice(connected_peers, id) == -1 {
// 				connected_peers = append(connected_peers, id)
// 			}
// 		}
// 	}
// 	return (len(connected_peers) + 1) > len(s.peers)/2
// }

func inSlice(s []uint64, e uint64) int {
	if s == nil {
		return -1
	}
	length := len(s)
	for i := 0; i < length; i++ {
		if s[i] == e {
			return i
		}
	}
	return -1
}

func createFile(dir, name string) (*os.File, error) {
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dir, name)
	return os.Create(path)
}

func startCPUProfile(f *os.File) error {
	if f == nil {
		return errors.New("nil file")
	}
	return pprof.StartCPUProfile(f)
}

func stopCPUProfile() {
	pprof.StopCPUProfile()
}

// debug function
func dumpStart(Start partialOrder.Start) {
	fmt.Printf("STATE: %+v\n", Start.States.State)
	fmt.Printf("TERM: %+v\n", Start.States.CurrentTerm)
	fmt.Printf("VOTEDFOR: %+v\n", Start.States.VotedFor)
	fmt.Printf("VOTES: %+v\n", Start.States.Votes)
	fmt.Printf("LOG:%+v\n", Start.States.Log)
	fmt.Printf("COMMITINDEX: %+v\n", Start.States.CommitIndex)
	fmt.Printf("MMATCHINDEX: %+v\n\n\n", Start.States.MmatchIndex)
	fmt.Printf("QUORUM: %+v, %+v\n\n\n", Start.States.Quorum[0], Start.States.Quorum[1])

	fmt.Printf("remaining message:\n\n")
	for i := 0; i < len(Start.Remaining_pending); i++ {
		fmt.Printf("%+v\n", Start.Remaining_pending[i])
	}
	fmt.Printf("\n\n\n\n")
}

func (s *Simulation) dump() {
	fmt.Printf("-----------------------------------------------states size is %+v--------------------------------------------------\n\n", len(s.States))
	for i := 0; i < len(s.States); i++ {
		dumpStart(*s.States[i])
	}
}
