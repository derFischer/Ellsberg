package protocol

import (
	"fmt"

	"go.nyu.edu/protosim/engine"
	"go.nyu.edu/protosim/network"
	"go.nyu.edu/protosim/zab/messagepb"
	"go.nyu.edu/protosim/zab/spec"
)

// Pending is a candidate's pending messages: Zab runs over TCP, so messages
// are delivered in order per connection.
type Pending = spec.Pending

type Candidate = engine.Candidate[spec.State, Pending, struct{}]

type EndState = spec.EndState

// Zab is the ZooKeeper protocol for the engine.
type Zab struct{}

var timeout = messagepb.Message{Type: messagepb.TIMEOUT}
var turntoleader = messagepb.Message{Type: messagepb.TURNTOLEADER}
var turntofollower = messagepb.Message{Type: messagepb.TURNTOFOLLOWER}
var localEvents = []*messagepb.Message{&timeout, &turntoleader, &turntofollower}

// newNetwork returns the network of a ZooKeeper server: a learner and its
// leader each record a TCP event when their connection is established.
func newNetwork() *network.FIFONet[messagepb.Message] {
	return network.NewFIFONet(
		func(m *messagepb.Message) uint64 { return m.From },
		func(m *messagepb.Message) bool { return m.Type == messagepb.TCP },
	)
}

func (z Zab) LocalEvents() []*messagepb.Message {
	return localEvents
}

// OnLocal closes the server's connections when it times out.
func (z Zab) OnLocal(ev *messagepb.Message, p Pending) Pending {
	if ev.Type == messagepb.TIMEOUT {
		return p.CloseConnections()
	}
	return p
}

func (z Zab) Reachable(s *spec.State, sets []Pending, e *messagepb.Message, t *EndState) (bool, spec.State, bool) {
	var none spec.State
	return spec.Enabled(s, sets, e, t), none, false
}

func (z Zab) Apply(s spec.State, e *messagepb.Message) spec.State {
	return spec.Apply(s, e)
}

func (z Zab) Inducing(s *spec.State, t *EndState) (spec.State, bool) {
	return *s, spec.Matched(*s, t)
}

func (z Zab) Equal(a *spec.State, b *spec.State) bool {
	return a.Equal(b)
}

func (z Zab) ApplyASAP(m *messagepb.Message, s *spec.State) bool {
	return spec.ApplyASAP(m, s)
}

func (z Zab) Accept(c *Candidate, m *messagepb.Message) bool {
	return true
}

func (z Zab) Prepare(c *Candidate, end *messagepb.Message, next *messagepb.Message) (EndState, *EndState) {
	var ahead *EndState
	if next != nil {
		ahead = &EndState{End: spec.Inference(next)}
	}
	return EndState{End: spec.Inference(end)}, ahead
}

func (z Zab) Finish(nc *Candidate, parent *Candidate, i int) {
	nc.S.GC()
}

func PrintState(s *spec.State) {
	fmt.Printf("STATE: %+v\n", *s)
	fmt.Printf("PacketsSync: %+v\n", s.PacketsSync)
	fmt.Printf("ELECTION STATE: %+v\n", *s.ElectionStates)
	if s.ElectionStates.AckLdRecv != nil {
		fmt.Printf("ACKLD: %+v\n", *s.ElectionStates.AckLdRecv)
	}
	if s.ElectionStates.Connecting != nil {
		fmt.Printf("CONNECTING: %+v\n", *s.ElectionStates.Connecting)
	}
	if s.ElectionStates.ConnectInfo != nil {
		fmt.Printf("CONNECT INFO: %+v\n", *s.ElectionStates.ConnectInfo)
	}
	fmt.Printf("ZABSTATE: %+v\n", s.ZabState)
	fmt.Printf("EPOCH: %+v\n", s.AcceptEpoch)
	fmt.Printf("OTHER EPOCH: %+v\n", s.Epoch)
	fmt.Printf("LAST COMMIT: %+v\n", s.LastCommitted)
	fmt.Printf("LOGS:\n")
	for _, l := range s.Log {
		fmt.Printf("[%+v], ", *l)
	}
	if s.ElectionStates.Election != nil {
		fmt.Printf("Election: %+v\n", s.ElectionStates.Election)
	}
	if s.ElectionStates.Learners != nil {
		fmt.Printf("Learners: %+v\n", *s.ElectionStates.Learners)
	}
}
