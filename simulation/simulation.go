package simulation

import (
	"fmt"
	"log"
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

type Simulation struct {
	mu     sync.Mutex // Lock to protect shared access to this peer's state
	me     uint64
	States []partialOrder.Path
	MsgCh  chan messagepb.Msg
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	total := len(peers)
	votes := make([]bool, total)
	matchIndex := make([]uint64, total)

	for i := 0; i < total; i++ {
		votes[i] = false
		matchIndex[i] = 0
	}

	//hack the initial state
	lg := make([]statepb.Command, 4)
	lg[0] = statepb.Command{CommandIndex: 0}
	lg[3] = statepb.Command{CommandIndex: 3, CommandTerm: 1}

	sl := new(Simulation)
	sl.me = id
	sl.States = make([]partialOrder.Path, 1)
	sl.States[0] = partialOrder.Path{End_s: state.State{Me: id, State: statepb.Follower, CurrentTerm: 1, VotedFor: state.NOONE, Votes: votes, Log: lg, CommitIndex: 3, MmatchIndex: matchIndex, Peers: peers}}
	sl.MsgCh = make(chan messagepb.Msg)

	go func() {
		for {
			select {
			case msg := <-sl.MsgCh:
				// if msg.Msg == nil {
				// 	log.Fatalf("This message is nil: %+v\n", msg)
				// }
				if msg.Send {
					sl.CheckMsg(msg)
				} else {
					//fmt.Printf("!!!!!!!!!!!!!!!!receive the msg: %+v\n", msg)
					sl.AddPendings(msg)
				}
			}
		}
	}()
	return sl
}

func (s *Simulation) CheckMsg(msg messagepb.Msg) {
	fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!start to check msg: %+v\n\n\n", msg)
	// s.dump()
	tmp_state := make([]partialOrder.Path, 0)
	for i := 0; i < len(s.States); i++ {
		paths := partialOrder.ExplorePaths(s.States[i].End_s, s.States[i].Remaining_pending, msg)
		// fmt.Printf("paths: %+v\n\n\n", paths)
		tmp_state = append(tmp_state, paths...)
	}
	if len(tmp_state) == 0 {
		fmt.Printf("No valid states for this message. The message: %+v\n", msg)
		// s.dump()
		// fmt.Printf("all possbile states: %+v\n msg is :%+v\n", s.States, msg)
		log.Fatalf("No valid states for this message.")
		s.dump()
	}
	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!after check, current states: %+v\n\n\n", tmp_state)
	s.States = tmp_state
	// fmt.Printf("all possbile states: %+v\n", len(s.States))
	s.dump()
	// fmt.Printf("states size is %+v\n\n", len(s.States))
}

func (s *Simulation) AddPendings(msg messagepb.Msg) {
	for i := 0; i < len(s.States); i++ {
		if partialOrder.Drop(msg, s.States[i].End_s) {
			continue
		}
		t, m, _ := messagepb.MsgInterpreter(msg)
		fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!add %+v %v to pendings\n\n\n", t, m)
		s.States[i].Remaining_pending = append(s.States[i].Remaining_pending, msg)
	}
}

func (s *Simulation) dump() {
	fmt.Printf("states size is %+v\n\n", len(s.States))
	for i := 0; i < len(s.States); i++ {
		fmt.Printf("%+v\n\n\n", s.States[i])
	}
}

// func (s *Simulation) HandleRaftMessages(msg raftpb.Message) interface{} {
// 	msg_itp, from, _, ctx := s.MsgInterpreter(msg)
// 	s.ConnectionAlive(from, ctx)
// 	switch msg.Type {
// 	case raftpb.MsgProp:
// 		if msg.Entries[0].Type != raftpb.EntryNormal {
// 			log.Fatalf("the prop type is not normal\n")
// 		}
// 		command := CommandInterpreter(msg.Entries[0])
// 		s.ClientRequestHandler(command.Command)
// 		//log.printf("!!!!!!!!!!!!!!leader state after prop: %+v\n, %+v\n", msg, s)
// 	case raftpb.MsgVoteResp:
// 		s.RequestVoteReplyHandler(msg_itp.(RequestVoteReply))
// 	case raftpb.MsgAppResp:
// 		s.AppendEntryReplyHandler(msg_itp.(AppendEntriesReply))
// 	case raftpb.MsgHeartbeatResp:
// 		s.AppendEntryReplyHandler(msg_itp.(AppendEntriesReply))
// 	case raftpb.MsgReadIndex:
// 		s.ReadIndexRequestHandler(msg_itp.(ReadIndexArgs))
// 	case raftpb.MsgReadIndexResp:
// 		s.ReadIndexReplyHandler(msg_itp.(ReadIndexReply))
// 	}
// 	return nil
// }
