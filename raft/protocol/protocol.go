package protocol

import (
	"fmt"
	"reflect"

	"go.nyu.edu/protosim/engine"
	"go.nyu.edu/protosim/network"
	"go.nyu.edu/protosim/raft/messagepb"
	"go.nyu.edu/protosim/raft/spec"
)

// Pending is a candidate's pending messages: Raft does not assume messages are
// delivered in order.
type Pending = network.Unordered[messagepb.Message]

// ClientBuffer maps client request ids to the commit index (or, for reads,
// the read index) when the request was received.
type ClientBuffer = *map[interface{}]uint64

type Candidate = engine.Candidate[spec.State, Pending, ClientBuffer]

type EndState = spec.EndState

// Raft is the Raft protocol (etcd or redis, by build tag) for the engine.
type Raft struct {
	contexts []messagepb.Ctx        // ReadIndex contexts of the outgoing message being checked
	valid    *map[messagepb.Ctx]int // contexts still in use by unanswered reads
}

var timeout = messagepb.Message{Type: messagepb.Timeout}
var checkLeaderEvent = messagepb.Message{Type: messagepb.CheckLeader}
var localEvents = []*messagepb.Message{&timeout, &checkLeaderEvent}

func (r *Raft) LocalEvents() []*messagepb.Message {
	return localEvents
}

func (r *Raft) OnLocal(ev *messagepb.Message, p Pending) Pending {
	return p
}

func (r *Raft) Reachable(s *spec.State, sets []Pending, e *messagepb.Message, t *EndState) (bool, spec.State, bool) {
	ok, next, ns := spec.Enabled(*s, e, t)
	return ok, ns, ok && next
}

func (r *Raft) Apply(s spec.State, e *messagepb.Message) spec.State {
	return spec.Apply(s, e)
}

func (r *Raft) Inducing(s *spec.State, t *EndState) (spec.State, bool) {
	return spec.Matched(*s, t)
}

func (r *Raft) Equal(a *spec.State, b *spec.State) bool {
	return a.Equal(b, nil)
}

func (r *Raft) ApplyASAP(m *messagepb.Message, s *spec.State) bool {
	return spec.ApplyASAP(m, s)
}

// Accept records client requests: the commit index when a write was received,
// and the index a read must wait for. Reads do not change the state and are
// not added to the pending messages.
func (r *Raft) Accept(c *Candidate, msg *messagepb.Message) bool {
	s := &c.S
	buffer := *c.X
	if msg.Type == messagepb.ClientKVRO {
		ci := s.CommitIndex
		log := s.GetCommand(ci)
		if log.CommandTerm == s.CurrentTerm {
			buffer[msg.ReqID] = s.CommitIndex
		} else {
			if s.GetLastCommandTerm() < s.CurrentTerm {
				buffer[msg.ReqID] = s.GetLastCommandIndex()
			} else {
				for _, e := range s.Log {
					if e.CommandTerm == s.CurrentTerm {
						buffer[msg.ReqID] = e.CommandIndex
					}
				}
			}
		}
		return false
	}
	if msg.Type == messagepb.ClientKVPut || msg.Type == messagepb.ClientConfChange || msg.Type == messagepb.ClientNormal {
		buffer[msg.ReqID] = s.CommitIndex
	}
	return true
}

// Prepare drops the requests of a failed client reply from the pending
// messages, and describes the states that can send end (and next).
func (r *Raft) Prepare(c *Candidate, end *messagepb.Message, next *messagepb.Message) (EndState, *EndState) {
	if end.Type == messagepb.ClientNormalResp && !end.Success {
		c.P = c.P.Remove(func(m *messagepb.Message) bool {
			return (m.Type == messagepb.ClientKVPut || m.Type == messagepb.ClientConfChange) && reflect.DeepEqual(m.ReqID, end.ReqID)
		})
	}
	target := EndState{End: c.S.Inference(*end), Connections: r.contexts, Req_index: clientReqIndex(c.X, end.ReqID)}
	var ahead *EndState
	if next != nil {
		ahead = &EndState{End: c.S.Inference(*next), Connections: r.contexts}
	}
	clientBufferGC(c)
	return target, ahead
}

func (r *Raft) Finish(nc *Candidate, parent *Candidate, i int) {
	nc.S.Truncate(r.valid)
	if i == 0 {
		nc.X = parent.X
	} else {
		nc.X = copyMap(parent.X)
	}
}

// ClientReqBufferLimit is the number of client request indices kept before
// the buffer is cleared. Requests whose index was cleared are checked with
// index 0, i.e. more weakly. The buffer is copied for every candidate after each
// sent message, so a larger limit makes checking slower.
const ClientReqBufferLimit = 100

// clientBufferGC clears the client request indices once more than
// ClientReqBufferLimit are stored.
func clientBufferGC(c *Candidate) {
	if len(*c.X) > ClientReqBufferLimit {
		m := make(map[interface{}]uint64)
		c.X = &m
	}
}

func clientReqIndex(b ClientBuffer, reqid interface{}) uint64 {
	v, ok := (*b)[reqid]
	if !ok {
		return 0
	}
	return v
}

func copyMap(m ClientBuffer) ClientBuffer {
	n := make(map[interface{}]uint64, len(*m))
	for k, v := range *m {
		n[k] = v
	}
	return &n
}

func PrintMessage(m messagepb.Message) {
	switch m.Type {
	case messagepb.MsgAppResp:
		fmt.Printf("Type: MsgAppResp, From: %+v, Term: %+v, Accept: %+v, MatchIndex: %+v, Ctx: %+v\n", m.From, m.Mterm, m.Success, m.MatchIndex, m.Ctx)
	case messagepb.MsgVoteResp:
		fmt.Printf("Type: MsgVoteResp, From: %+v, Term: %+v, Accept: %+v, Ctx: %+v\n", m.From, m.Mterm, m.Success, m.Ctx)
	case messagepb.CheckLeader:
		fmt.Printf("Type: checkLeader\n")
	case messagepb.CheckCommitIndex:
		fmt.Printf("Type: checkCommitIndex\n")
	case messagepb.Timeout:
		fmt.Printf("Type: timeout\n")
	case messagepb.ClientNormal, messagepb.ClientKVPut:
		fmt.Printf("Type: clientReq, ID: %+v\n", m.ReqID)
	case messagepb.ClientConfChange:
		fmt.Printf("Type: clientReqConf, ID: %+v, changes: %+v\n", m.ReqID, m.ConfChange)
	default:
		fmt.Printf("UNKNOWN MESSAGE! %+v\n", m)
	}
}
