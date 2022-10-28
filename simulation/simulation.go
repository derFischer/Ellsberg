package simulation

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"go.etcd.io/etcd/v3/messagepb"
	"go.etcd.io/etcd/v3/partialOrder"
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
	States      []partialOrder.Start
	MsgCh       chan messagepb.Msg
	Connections map[messagepb.Ctx]Connection
	ROReq       []ROEntry
	ReadIndex   map[messagepb.Ctx]RIndex
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	matchIndex := make([]uint64, total)
	alive := make(map[messagepb.Ctx]Connection)
	rindex := make(map[messagepb.Ctx]RIndex)

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
	sl.States = make([]partialOrder.Start, 1)
	sl.States[0] = partialOrder.Start{States: state.State{Me: id, State: statepb.Follower, CurrentTerm: 1, VotedFor: state.NOONE, Votes: votes, Log: lg, CommitIndex: 3, MmatchIndex: matchIndex, Peers: peers}}
	sl.MsgCh = make(chan messagepb.Msg)
	sl.Connections = alive
	sl.ReadIndex = rindex

	sl.ROReq = make([]ROEntry, 0)

	go func() {
		for {
			select {
			case msg := <-sl.MsgCh:
				// if msg.Msg == nil {
				// 	log.Fatalf("This message is nil: %+v\n", msg)
				// }
				if msg.Send {
					sl.sendMsg(msg)
				} else {
					//fmt.Printf("!!!!!!!!!!!!!!!!receive the msg: %+v\n", msg)
					sl.receiveMsg(msg)
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
	// fmt.Printf("the states after sending the message: %+v\n\n\n", s.States)
}

func (s *Simulation) receiveMsg(message messagepb.Msg) {
	msg, ctx := messagepb.MsgInterpreter(message)
	// if msg.Type != messagepb.MsgAppResp {
	// 	fmt.Printf("receive read index resp: %+v\n", msg)
	// }
	s.ROReceivePreprocess(msg, ctx)
	s.AddPendings(msg)
}

func (s *Simulation) CheckMsg(msg messagepb.Message, connected_peers []uint64, valid_indice []uint64) {
	tmp_state := make([]partialOrder.Start, 0)
	for i := 0; i < len(s.States); i++ {
		Starts := partialOrder.ExplorePaths(s.States[i].States, s.States[i].Remaining_pending, msg, connected_peers, valid_indice)
		tmp_state = append(tmp_state, Starts...)
	}
	if len(tmp_state) == 0 {
		// fmt.Printf("No valid states for this message. The message: %+v\n ", msg)
		// s.dump()
		log.Fatalf("No valid states for this message: %+v\n", msg)
	}
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
		s.States[i].Remaining_pending = append(s.States[i].Remaining_pending, msg)
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

//helper function
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

//debug function
func dumpStart(Start partialOrder.Start) {
	fmt.Printf("STATE: %+v\n", Start.States.State)
	fmt.Printf("TERM: %+v\n", Start.States.CurrentTerm)
	fmt.Printf("VOTEDFOR: %+v\n", Start.States.VotedFor)
	fmt.Printf("VOTES: %+v\n", Start.States.Votes)
	fmt.Printf("LOG:%+v\n", Start.States.Log)
	fmt.Printf("COMMITINDEX: %+v\n", Start.States.CommitIndex)
	fmt.Printf("MMATCHINDEX: %+v\n\n\n", Start.States.MmatchIndex)

	fmt.Printf("remaining message:\n\n")
	for i := 0; i < len(Start.Remaining_pending); i++ {
		fmt.Printf("%+v\n", Start.Remaining_pending[i])
	}
	fmt.Printf("\n\n\n\n")
}

func (s *Simulation) dump() {
	fmt.Printf("-----------------------------------------------states size is %+v--------------------------------------------------\n\n", len(s.States))
	for i := 0; i < len(s.States); i++ {
		dumpStart(s.States[i])
	}
}
