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
}

type ROEntry struct {
	ReqID interface{}
	Ctxs  []messagepb.Ctx
}

type Simulation struct {
	mu          sync.Mutex // Lock to protect shared access to this peer's state
	me          uint64
	States      []partialOrder.Path
	MsgCh       chan messagepb.Msg
	Connections map[messagepb.Ctx]Connection
	ROReq       []ROEntry
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	matchIndex := make([]uint64, total)
	alive := make(map[messagepb.Ctx]Connection)

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
	sl.States = make([]partialOrder.Path, 1)
	sl.States[0] = partialOrder.Path{End_s: state.State{Me: id, State: statepb.Follower, CurrentTerm: 1, VotedFor: state.NOONE, Votes: votes, Log: lg, CommitIndex: 3, MmatchIndex: matchIndex, Peers: peers}}
	sl.MsgCh = make(chan messagepb.Msg)
	sl.Connections = alive

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

func (s *Simulation) sendMsg(msg messagepb.Msg) {
	_, _, ctx := messagepb.GetCtx(msg)
	s.InitializeCtx(ctx)

	connected_peers := []uint64{}
	t, m, _ := messagepb.MsgInterpreter(msg)
	ROResp := (t == messagepb.ClientResp) && (m.(messagepb.Resp).Type == messagepb.ReadResp)
	if ROResp {
		found, entry := s.searchInROReq(m.(messagepb.Resp).ID)
		if !found {
			log.Fatalf("haven't received the client req for response %+v before\n", m.(messagepb.Resp))
		}
		for i := 0; i < len(entry.Ctxs); i++ {
			ctx := entry.Ctxs[i]
			connection := s.Connections[ctx]
			for _, id := range connection.ids {
				if inSlice(connected_peers, id) == -1 {
					connected_peers = append(connected_peers, id)
				}
			}
		}
		fmt.Printf("connected peers after this read: %+v\n", connected_peers)
	}
	s.CheckMsg(msg, connected_peers)
}

func (s *Simulation) receiveMsg(msg messagepb.Msg) {
	from, _, ctx := messagepb.GetCtx(msg)
	s.ConnectionAlive(from, ctx)

	t, m, _ := messagepb.MsgInterpreter(msg)
	if t == messagepb.ClientRequest {
		if m.(statepb.Op).Type == statepb.EntryKVRO {
			s.ROReq = append(s.ROReq, ROEntry{ReqID: m.(statepb.Op).ReqID})
		}
	}
	s.AddPendings(msg)
}

func (s *Simulation) CheckMsg(msg messagepb.Msg, connected_peers []uint64) {
	tmp_state := make([]partialOrder.Path, 0)
	for i := 0; i < len(s.States); i++ {
		paths := partialOrder.ExplorePaths(s.States[i].End_s, s.States[i].Remaining_pending, msg, connected_peers)
		// fmt.Printf("paths: %+v\n\n\n", paths)
		tmp_state = append(tmp_state, paths...)
	}
	if len(tmp_state) == 0 {
		fmt.Printf("No valid states for this message. The message: %+v\n", msg)
		// s.dump()
		// s.dump()
		// fmt.Printf("all possbile states: %+v\n msg is :%+v\n", s.States, msg)
		log.Fatalf("No valid states for this message.")
	}
	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!after check, current states: %+v\n\n\n", tmp_state)
	s.States = tmp_state
	// fmt.Printf("all possbile states: %+v\n", len(s.States))
	// s.dump()
	// fmt.Printf("states size is %+v\n\n", len(s.States))
}

func (s *Simulation) InitializeCtx(ctx messagepb.Ctx) {
	if ctx == nil {
		return
	}
	fmt.Printf("initialize ctx: %+v, current pending ro reqs: %+v\n", ctx, s.ROReq)
	found := s.searchConnection(ctx, true, len(s.ROReq))
	if found {
		return
	}
	for i := 0; i < len(s.ROReq); i++ {
		entry := s.ROReq[i]
		s.ROReq[i] = ROEntry{ReqID: entry.ReqID, Ctxs: append(entry.Ctxs, ctx)}
	}
}

func (s *Simulation) ConnectionAlive(id uint64, ctx messagepb.Ctx) {
	if ctx == nil {
		return
	}
	fmt.Printf("connection alive: %+v\n", ctx)
	found := s.searchConnection(ctx, false, 0)
	if found {
		connection := s.Connections[ctx]
		if inSlice(connection.ids, id) == -1 {
			s.Connections[ctx] = Connection{ref: connection.ref, ids: append(connection.ids, id)}
		}
	}
}

func (s *Simulation) AddPendings(msg messagepb.Msg) {
	for i := 0; i < len(s.States); i++ {
		if partialOrder.Drop(msg, s.States[i].End_s) {
			continue
		}
		// t, m, _ := messagepb.MsgInterpreter(msg)
		// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!add %+v %v to pendings\n\n\n", t, m)
		s.States[i].Remaining_pending = append(s.States[i].Remaining_pending, msg)
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
func dumpPath(path partialOrder.Path) {
	fmt.Printf("STATE: %+v\n", path.End_s.State)
	fmt.Printf("TERM: %+v\n", path.End_s.CurrentTerm)
	fmt.Printf("VOTEDFOR: %+v\n", path.End_s.VotedFor)
	fmt.Printf("VOTES: %+v\n", path.End_s.Votes)
	fmt.Printf("LOG:%+v\n", path.End_s.Log)
	fmt.Printf("COMMITINDEX: %+v\n", path.End_s.CommitIndex)
	fmt.Printf("MMATCHINDEX: %+v\n\n\n", path.End_s.MmatchIndex)

	fmt.Printf("remaining message:\n\n")
	for i := 0; i < len(path.Remaining_pending); i++ {
		fmt.Printf("%+v\n", path.Remaining_pending[i])
	}
	fmt.Printf("\n\n\n\n")
}

func (s *Simulation) dump() {
	fmt.Printf("-----------------------------------------------states size is %+v--------------------------------------------------\n\n", len(s.States))
	for i := 0; i < len(s.States); i++ {
		dumpPath(s.States[i])
	}
}
