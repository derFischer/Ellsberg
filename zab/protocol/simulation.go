//go:build zk

package protocol

import (
	"fmt"
	"log"

	"go.nyu.edu/protosim/engine"
	"go.nyu.edu/protosim/statistics"
	"go.nyu.edu/protosim/zab/messagepb"
	"go.nyu.edu/protosim/zab/spec"
)

// Simulation checks the trace of one ZooKeeper server.
type Simulation struct {
	me      uint64
	checker *engine.Checker[spec.State, messagepb.Message, Pending, EndState, struct{}]
}

func (s *Simulation) SendMsg(msg *messagepb.Message, next_message *messagepb.Message) (success bool) {
	if msg.Type == messagepb.NOT {
		return true
	}
	if !s.checker.Send(msg, next_message) {
		s.dump()
		log.Printf("No valid states for this message: %+v\n", msg)
		return false
	}
	statistics.Stats.RecordMsgType(int8(msg.Type))
	return true
}

func (s *Simulation) ReceiveMsg(msg messagepb.Message) {
	if msg.Type == messagepb.NOT {
		return
	}
	s.checker.Receive(&msg)

	max_pendings_count := 0
	for _, c := range s.checker.Candidates {
		if n := c.P.Len(); n > max_pendings_count {
			max_pendings_count = n
		}
	}
	statistics.Stats.RecordRemainingStates(uint64(len(s.checker.Candidates)))
	statistics.Stats.RecordPendingMsgCount(uint64(max_pendings_count))
	statistics.Stats.RecordMsgType(int8(msg.Type))
}

func StartSimulationServer(id uint64, peers []uint64) *Simulation {
	initial := &Candidate{S: spec.InitialState(id, peers)}
	return &Simulation{
		me: id,
		checker: &engine.Checker[spec.State, messagepb.Message, Pending, EndState, struct{}]{
			Proto:      Zab{},
			Net:        newNetwork(),
			Candidates: []*Candidate{initial},
		},
	}
}

func (s *Simulation) Dump() {
	s.dump()
}

func (s *Simulation) dump() {
	for _, c := range s.checker.Candidates {
		dumpCandidate(c)
		fmt.Printf("---\n")
	}
}

func dumpCandidate(c *Candidate) {
	st := &c.S
	fmt.Printf("State: %+v\n", *st)
	fmt.Printf("PacketSync: %+v\n", st.PacketsSync)
	c.P.Dump()
	fmt.Printf("Log:\n")
	for _, entry := range st.Log {
		fmt.Printf("\t%+v\n", *entry)
	}
	fmt.Printf("Election state: %+v\n", *st.ElectionStates)
	fmt.Printf("Connecting: %+v\n", *st.ElectionStates.Connecting)
	fmt.Printf("ConnectInfo: %+v\n", *st.ElectionStates.ConnectInfo)
	fmt.Printf("Learners: %+v\n", *st.ElectionStates.Learners)
	fmt.Printf("AckLdRecv: %+v\n", *st.ElectionStates.AckLdRecv)
	fmt.Printf("ZABSTATE: %+v\n", st.ZabState)
	fmt.Printf("EPOCH: %+v\n", st.AcceptEpoch)
	fmt.Printf("LAST COMMIT: %+v\n", st.LastCommitted)
	fmt.Printf("LAST ENTRY OF LOG: %+v\n", st.Log[len(st.Log)-1])
}
