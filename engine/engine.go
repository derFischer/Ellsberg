// Package engine is the protocol-independent part of the checker: the set of
// candidate states (simulation states in the paper) kept while replaying a
// server's trace, and the search for the states that explain each outgoing
// message (find_reachable, paper Listings 1 and 2).
//
// A protocol provides its specification (Spec) and how it reads its trace
// (Protocol); the network model (package network) decides which pending
// messages can be delivered and when one pending set covers another.
package engine

import (
	"go.nyu.edu/protosim/network"
	"go.nyu.edu/protosim/statistics"
)

// Experiment settings, for evaluating the optimizations (paper Appendix).
type MERGE_MODE_TYPE int8

const (
	NOMERGE    MERGE_MODE_TYPE = 0
	EXACT      MERGE_MODE_TYPE = 1
	FUNCTIONAL MERGE_MODE_TYPE = 2
)

type ASAP_MODE_TYPE int8

const (
	OFF ASAP_MODE_TYPE = 0
	ON  ASAP_MODE_TYPE = 1
)

var MergeMode = FUNCTIONAL
var ASAPMode = ON

// trimInterval is the number of checked outgoing messages between two calls
// to Network.Trim.
const trimInterval = 1024

// Candidate is a candidate state of the server: a protocol state S, the
// pending messages P, and protocol data X kept alongside (not part of the
// protocol state, e.g. client request bookkeeping).
type Candidate[S any, P any, X any] struct {
	S S
	P P
	X X
}

// Spec is a protocol specification (paper Section 3.2). T is the protocol's
// description of the states that can send a given message (infer_inducing).
type Spec[S any, M any, P any, T any] interface {
	// LocalEvents returns the events a server can take on its own, such as
	// timeouts. They are tried before pending messages.
	LocalEvents() []*M
	// OnLocal returns the pending messages after local event ev (e.g. a
	// timeout that closes connections).
	OnLocal(ev *M, p P) P
	// Reachable reports whether a state described by t may still be
	// reachable after applying e to s, whose pending sets are sets
	// (reachable). If it already knows the state e leads to, it returns it
	// with known set.
	Reachable(s *S, sets []P, e *M, t *T) (ok bool, next S, known bool)
	// Apply applies event e to s.
	Apply(s S, e *M) S
	// Inducing reports whether s is described by t, returning s as needed
	// to send the message (e.g. with a reconfiguration applied).
	Inducing(s *S, t *T) (S, bool)
	// Equal reports whether two states behave the same.
	Equal(a *S, b *S) bool
	// ApplyASAP applies m to s if it can be applied right away without
	// losing behaviors (apply_asap?), and reports whether it did.
	ApplyASAP(m *M, s *S) bool
}

// Protocol is a Spec together with how the protocol's trace is checked.
type Protocol[S any, M any, P any, T any, X any] interface {
	Spec[S, M, P, T]
	// Accept is called for each candidate when m is received, before m is
	// added to the candidate's pending messages; false keeps m out of them.
	Accept(c *Candidate[S, P, X], m *M) bool
	// Prepare returns, for candidate c, the states that can send m and, if
	// next is not nil, those that can send the later message next (look
	// ahead). It may update c.
	Prepare(c *Candidate[S, P, X], m *M, next *M) (target T, ahead *T)
	// Finish completes a candidate found for an outgoing message from
	// parent; i numbers the candidates found from the same parent.
	Finish(r *Candidate[S, P, X], parent *Candidate[S, P, X], i int)
}

// Checker keeps the candidate states while a server's trace is replayed.
type Checker[S any, M any, P network.Pending[M, P], T any, X any] struct {
	Proto      Protocol[S, M, P, T, X]
	Net        network.Network[M, P]
	Candidates []*Candidate[S, P, X]

	sends int

	// Scratch space, reused so that checking a message does not allocate:
	// values passed by pointer to the protocol escape to the heap.
	scratch   S               // the state being added to the search
	asapOn    *S              // the state ASAP messages are applied to
	asapApply func(m *M) bool // applies m to *asapOn
	target    T               // the target of the current search
	explored  []bool          // flags of the node being explored
}

// Receive processes an incoming message from the trace (process_incoming).
func (c *Checker[S, M, P, T, X]) Receive(m *M) {
	a := c.Net.Receive(m)
	for _, cand := range c.Candidates {
		if !c.Proto.Accept(cand, m) {
			continue
		}
		c.asapOn = &cand.S
		cand.P, _, _ = cand.P.Received(a).Saturate(c.applyASAP())
	}
}

// applyASAP returns the function that applies a message to *asapOn if it can
// be applied right away.
func (c *Checker[S, M, P, T, X]) applyASAP() func(m *M) bool {
	if c.asapApply == nil {
		c.asapApply = func(m *M) bool {
			if ASAPMode == OFF && statistics.Stats.MsgProcessed >= statistics.Stats.HackStart {
				return false
			}
			return c.Proto.ApplyASAP(m, c.asapOn)
		}
	}
	return c.asapApply
}

// Send processes an outgoing message m from the trace (process_outgoing):
// the candidates become the states reachable from them that can send m. next
// is the message to look ahead to, or nil. It reports whether any candidate
// can send m; if none can, the candidates are left unchanged.
func (c *Checker[S, M, P, T, X]) Send(m *M, next *M) bool {
	statistics.Stats.StateCountThisSend = 0
	var out []*Candidate[S, P, X]
	found, maxPending := 0, 0
	for _, cand := range c.Candidates {
		var ahead *T
		c.target, ahead = c.Proto.Prepare(cand, m, next)
		for i, r := range c.findReachable(cand.S, cand.P, &c.target, ahead) {
			nc := &Candidate[S, P, X]{S: r.s, P: r.p}
			c.Proto.Finish(nc, cand, i)
			found++
			if n := nc.P.Len(); n > maxPending {
				maxPending = n
			}
			out = c.merge(out, nc)
		}
	}
	if len(out) == 0 {
		return false
	}
	statistics.Stats.RecordEliminatedStates(uint64(found - len(out)))
	statistics.Stats.RecordRemainingStates(uint64(len(out)))
	statistics.Stats.RecordPendingMsgCount(uint64(maxPending))
	statistics.Stats.RecordStateCountPerSend(statistics.Stats.StateCountThisSend)
	c.Candidates = out
	c.sends++
	if c.sends%trimInterval == 0 {
		pendings := make([]P, len(out))
		for i, cand := range out {
			pendings[i] = cand.P
		}
		c.Net.Trim(pendings)
	}
	return true
}

// merge adds r to the candidates, unless a candidate with an equal state
// covers its pending messages; candidates with an equal state whose pending
// messages r covers are dropped.
func (c *Checker[S, M, P, T, X]) merge(out []*Candidate[S, P, X], r *Candidate[S, P, X]) []*Candidate[S, P, X] {
	if MergeMode == NOMERGE && statistics.Stats.MsgProcessed >= statistics.Stats.HackStart {
		return append(out, r)
	}
	for i := 0; i < len(out); i++ {
		if !c.Proto.Equal(&r.S, &out[i].S) {
			continue
		}
		if r.P.Covers(out[i].P) {
			out = append(out[:i], out[i+1:]...)
			i--
		} else if out[i].P.Covers(r.P) {
			return out
		}
	}
	return append(out, r)
}

type reached[S any, P any] struct {
	s S
	p P
}

// node is a protocol state in the search, with the pending sets it is
// explored with.
type node[S any, P any] struct {
	s    S
	sets []P
}

// findReachable returns the states reachable from (start, pending) that are
// described by target, and by ahead if it is not nil (paper Listing 2).
//
// Nodes are explored in breadth-first order. A state equal to one waiting to
// be explored adds its pending set to that node; a state equal to one already
// explored is explored again only if no pending set it was explored with
// covers the new one.
func (c *Checker[S, M, P, T, X]) findReachable(start S, pending P, target *T, ahead *T) []reached[S, P] {
	sp := c.Proto
	c.scratch = start
	nodes := []*node[S, P]{{sets: []P{c.saturate(pending)}}}
	nodes[0].s = c.scratch
	newNodes := 0
	var result []reached[S, P]
	for cur := 0; cur < len(nodes); cur++ {
		n := nodes[cur]
		if s, ok := sp.Inducing(&n.s, target); ok {
			for _, p := range n.sets {
				c.scratch = s
				p = c.saturate(p)
				result = append(result, reached[S, P]{s: c.scratch, p: p})
			}
			continue
		}
		edges, pruned, pruned1, pruned2 := 0, 0, 0, 0
		step := func(e *M) (S, bool) {
			edges++
			ok1, next, known := sp.Reachable(&n.s, n.sets, e, target)
			ok2 := true
			if ahead != nil {
				ok2, _, _ = sp.Reachable(&n.s, n.sets, e, ahead)
			}
			if !ok1 || !ok2 {
				pruned++
				if ok1 {
					pruned1++
				}
				if ok2 {
					pruned2++
				}
				var none S
				return none, false
			}
			if known {
				return next, true
			}
			return sp.Apply(n.s, e), true
		}
		insert := func(s S, p P) {
			c.scratch = s
			newNodes += c.insert(&nodes, cur, c.saturate(p))
		}

		for _, ev := range sp.LocalEvents() {
			s, ok := step(ev)
			if !ok {
				continue
			}
			for _, p := range n.sets {
				insert(s, sp.OnLocal(ev, p))
			}
		}

		// Each message is applied once, for every pending set it can be
		// delivered from. explored[base[j]+i] marks slot i of set j.
		total := 0
		for _, p := range n.sets {
			total += p.Slots()
		}
		if cap(c.explored) < total {
			c.explored = make([]bool, total)
		}
		explored := c.explored[:total]
		for i := range explored {
			explored[i] = false
		}
		base := 0
		for j, p := range n.sets {
			for i := 0; i < p.Slots(); i++ {
				m := p.Next(i)
				if m == nil || explored[base+i] {
					continue
				}
				explored[base+i] = true
				s, ok := step(m)
				if ok {
					insert(s, p.Deliver(i))
				}
				base2 := base + p.Slots()
				for k := j + 1; k < len(n.sets); k++ {
					if i2 := n.sets[k].Find(m); i2 >= 0 {
						explored[base2+i2] = true
						if ok {
							insert(s, n.sets[k].Deliver(i2))
						}
					}
					base2 += n.sets[k].Slots()
				}
			}
			base += p.Slots()
		}
		statistics.Stats.RecordPrunedRatio(float64(pruned) / float64(edges))
		statistics.Stats.RecordPruned1Ratio(float64(pruned1) / float64(edges))
		statistics.Stats.RecordPruned2Ratio(float64(pruned2) / float64(edges))
	}
	statistics.Stats.StateCountThisSend += uint64(newNodes)
	return result
}

// insert adds (c.scratch, p) to the search and returns the number of nodes
// added (0 or 1). cur is the node being explored; nodes after it are waiting.
func (c *Checker[S, M, P, T, X]) insert(nodes *[]*node[S, P], cur int, p P) int {
	var waiting *node[S, P]
	covered := false
	for k, n := range *nodes {
		if !c.Proto.Equal(&n.s, &c.scratch) {
			continue
		}
		if k > cur {
			waiting = n
			break
		}
		for _, q := range n.sets {
			if q.Covers(p) {
				covered = true
				break
			}
		}
	}
	if waiting != nil {
		waiting.sets = mergePending(waiting.sets, p)
		return 0
	}
	if covered {
		return 0
	}
	*nodes = append(*nodes, &node[S, P]{s: c.scratch, sets: []P{p}})
	return 1
}

// mergePending adds p to the pending sets of a node, keeping only sets no
// other set covers.
func mergePending[P interface{ Covers(P) bool }](sets []P, p P) []P {
	out := make([]P, 0, len(sets)+1)
	found := false
	for _, q := range sets {
		if q.Covers(p) {
			found = true
		} else if p.Covers(q) {
			continue
		}
		out = append(out, q)
	}
	if !found {
		out = append(out, p)
	}
	return out
}

// saturate applies to c.scratch the pending messages that can be applied
// right away (prune_asap) and returns the others.
func (c *Checker[S, M, P, T, X]) saturate(p P) P {
	if p.Slots() == 0 {
		return p
	}
	c.asapOn = &c.scratch
	rest, delivered, tried := p.Saturate(c.applyASAP())
	if tried != 0 {
		statistics.Stats.RecordPendingRatio(float64(delivered) / float64(tried))
	}
	return rest
}
