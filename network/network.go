// Package network models the messages a server has received but not yet
// processed: the pending messages of a candidate state. The engine only uses
// the Pending and Network interfaces, so the delivery rules of the network
// (any order, or in order per connection) stay out of the search.
package network

// Pending is one candidate's pending messages. A Pending is never modified:
// every operation returns a new one, so candidates can share them.
//
// The messages that can be delivered next are reached through slots: an
// unordered network has a slot per pending message, a FIFO network a slot per
// connection, holding the connection's next message.
type Pending[M any, P any] interface {
	// Received returns the pending messages after a was received.
	Received(a Arrival[M]) P
	// Len returns the number of pending messages.
	Len() int
	// Slots returns the number of slots.
	Slots() int
	// Next returns the message slot i can deliver now, or nil.
	Next(i int) *M
	// Find returns the slot that can deliver m now, or -1.
	Find(m *M) int
	// Deliver returns the pending messages after delivering Next(i).
	Deliver(i int) P
	// Covers reports whether o allows no behavior that p does not: a
	// candidate with p pending can do anything one with the same state and
	// o pending can do.
	Covers(o P) bool
	// Saturate delivers the messages apply accepts (apply applies them to
	// the candidate's state, see apply_asap? in the paper) and returns the
	// remaining messages, with the number of messages delivered and the
	// number apply was called on.
	Saturate(apply func(*M) bool) (rest P, delivered int, tried int)
	// Each calls f on every pending message until f returns false.
	Each(f func(*M) bool)
}

// Network records the messages a server received, for all candidates.
type Network[M any, P any] interface {
	// Receive records that m was received and returns it for
	// Pending.Received.
	Receive(m *M) Arrival[M]
	// Trim is called with the pending messages of every candidate, so that
	// the network can free the messages none of them can deliver any more.
	Trim(pendings []P)
}

// Arrival is a received message, as recorded by a Network.
type Arrival[M any] struct {
	Msg *M
	st  *stream[M]
	off int
}
