package network

import "fmt"

// FIFO is a network of connections, such as TCP: the messages of one
// connection are delivered in the order they were received, and connections
// are independent of each other. A connection is opened by a message that the
// protocol marks as opening one (for ZooKeeper, the TCP event a learner and its
// leader record when they connect), and that message is the connection's first.
// Messages from a sender that has never opened a connection (for ZooKeeper,
// client requests, by session) form one connection per sender.
//
// Every received message is stored once, in the stream of its connection,
// which all candidates share. A candidate's pending messages are a cursor for
// each connection it can still receive from, in the order it learned of them.
type FIFO[M any] []cursor[M]

// FIFONet is the Network of FIFO pending messages.
type FIFONet[M any] struct {
	sender func(*M) uint64 // the peer a message comes from
	opens  func(*M) bool   // whether a message opens a new connection

	latest map[uint64]*stream[M] // the newest stream of each peer

	trimEpoch uint64
	live      []*stream[M]
}

// stream is the messages received on one connection, in order.
type stream[M any] struct {
	sid   uint64
	opens bool // opened by a message, which is at offset 0

	base int // offset of msgs[0]; earlier messages were delivered or dropped by every candidate
	msgs []*M

	trimEpoch uint64
	keep      int
}

type cursor[M any] struct {
	st   *stream[M]
	head int // offset of the next message to deliver
}

func NewFIFONet[M any](sender func(*M) uint64, opens func(*M) bool) *FIFONet[M] {
	return &FIFONet[M]{sender: sender, opens: opens, latest: make(map[uint64]*stream[M])}
}

func (st *stream[M]) end() int {
	return st.base + len(st.msgs)
}

func (st *stream[M]) at(off int) *M {
	return st.msgs[off-st.base]
}

// Receive stores m in the stream of its connection: a message that opens a
// connection starts a new stream, any other goes to the newest stream of its
// sender.
func (n *FIFONet[M]) Receive(m *M) Arrival[M] {
	sid := n.sender(m)
	opens := n.opens(m)
	st := n.latest[sid]
	if opens || st == nil {
		st = &stream[M]{sid: sid, opens: opens}
		n.latest[sid] = st
	}
	st.msgs = append(st.msgs, m)
	return Arrival[M]{Msg: m, st: st, off: st.end() - 1}
}

// Trim frees the messages that no candidate can deliver any more.
func (n *FIFONet[M]) Trim(pendings []FIFO[M]) {
	n.trimEpoch++
	n.live = n.live[:0]
	for _, p := range pendings {
		for _, c := range p {
			st := c.st
			if st.trimEpoch != n.trimEpoch {
				st.trimEpoch = n.trimEpoch
				st.keep = c.head
				n.live = append(n.live, st)
			} else if c.head < st.keep {
				st.keep = c.head
			}
		}
	}
	for _, st := range n.live {
		st.trim(st.keep)
	}
	// A stream no candidate has a cursor on can still get one when its
	// sender sends again (see FIFO.Received), starting at that message.
	for _, st := range n.latest {
		if st.trimEpoch != n.trimEpoch {
			st.trim(st.end())
		}
	}
}

func (st *stream[M]) trim(off int) {
	k := off - st.base
	if k <= 0 {
		return
	}
	for i := 0; i < k; i++ {
		st.msgs[i] = nil
	}
	st.msgs = st.msgs[k:]
	st.base = off
}

func (c cursor[M]) len() int {
	return c.st.end() - c.head
}

// unaccepted reports whether the message that opens the connection has not
// been delivered yet.
func (c cursor[M]) unaccepted() bool {
	return c.st.opens && c.head == 0 && c.len() > 0
}

// Received makes the message pending through the candidate's cursor on its
// stream. A candidate with no connection from the sender gets a new cursor,
// starting at the message.
func (p FIFO[M]) Received(a Arrival[M]) FIFO[M] {
	if !(a.st.opens && a.off == 0) {
		for i := len(p) - 1; i >= 0; i-- {
			if p[i].st.sid == a.st.sid {
				if p[i].st != a.st {
					panic(fmt.Sprintf("message from %d received on an older connection", a.st.sid))
				}
				return p
			}
		}
	}
	q := make(FIFO[M], len(p), len(p)+1)
	copy(q, p)
	return append(q, cursor[M]{st: a.st, head: a.off})
}

func (p FIFO[M]) Len() int {
	total := 0
	for _, c := range p {
		total += c.len()
	}
	return total
}

func (p FIFO[M]) Slots() int {
	return len(p)
}

func (p FIFO[M]) Next(i int) *M {
	if p[i].len() == 0 {
		return nil
	}
	return p[i].st.at(p[i].head)
}

func (p FIFO[M]) Find(m *M) int {
	for i, c := range p {
		if c.len() > 0 && c.st.at(c.head) == m {
			return i
		}
	}
	return -1
}

// Deliver delivers the next message of connection i. Delivering the message
// that opens a connection accepts the connection, which closes the older
// connections from the same peer.
func (p FIFO[M]) Deliver(i int) FIFO[M] {
	accept := p[i].unaccepted()
	q := make(FIFO[M], 0, len(p))
	for j, c := range p {
		if j < i && accept && c.st.sid == p[i].st.sid {
			continue
		}
		if j == i {
			c.head++
		}
		q = append(q, c)
	}
	return q.normal()
}

// CloseConnections closes the current connection to each peer, dropping the
// messages pending on it (for ZooKeeper, a server that times out). Connections
// that have not been accepted yet are kept.
func (p FIFO[M]) CloseConnections() FIFO[M] {
	q := make(FIFO[M], 0, len(p))
	closed := make(map[uint64]bool, 5)
	for _, c := range p {
		if c.unaccepted() {
			closed[c.st.sid] = true
		}
		if closed[c.st.sid] {
			q = append(q, c)
			continue
		}
		closed[c.st.sid] = true
	}
	return q.normal()
}

// Saturate passes over the connections, delivering each one's next message if
// apply accepts it, until a pass delivers nothing.
func (p FIFO[M]) Saturate(apply func(*M) bool) (FIFO[M], int, int) {
	var heads []int
	delivered, tried := 0, 0
	for {
		progressed := false
		for i, c := range p {
			head := c.head
			if heads != nil {
				head = heads[i]
			}
			if head >= c.st.end() {
				continue
			}
			tried++
			if apply(c.st.at(head)) {
				if heads == nil {
					heads = make([]int, len(p))
					for j := range p {
						heads[j] = p[j].head
					}
				}
				heads[i]++
				delivered++
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	if heads == nil {
		return p, 0, tried
	}
	q := make(FIFO[M], len(p))
	for i, c := range p {
		q[i] = cursor[M]{st: c.st, head: heads[i]}
	}
	return q.normal(), delivered, tried
}

// normal returns p, or nil if no message is pending. Without pending
// messages, a candidate's cursors make no difference: a message received
// later from a peer is pending either through the cursor or through a new one.
func (p FIFO[M]) normal() FIFO[M] {
	if p.Len() == 0 {
		return nil
	}
	return p
}

// Covers reports whether p and o are equal: the same connections, in the same
// order, with the same pending messages. A larger FIFO pending set does not
// cover a smaller one: an extra message at the head of a connection blocks the
// messages behind it.
func (p FIFO[M]) Covers(o FIFO[M]) bool {
	if len(p) != len(o) {
		return false
	}
	for i := range p {
		a, b := p[i], o[i]
		if a.st.sid != b.st.sid {
			return false
		}
		if a.len() == 0 && b.len() == 0 {
			continue
		}
		if a.st != b.st || a.head != b.head {
			return false
		}
	}
	return true
}

func (p FIFO[M]) Each(f func(*M) bool) {
	for _, c := range p {
		for off := c.head; off < c.st.end(); off++ {
			if !f(c.st.at(off)) {
				return
			}
		}
	}
}

// Dump prints the pending messages, by connection.
func (p FIFO[M]) Dump() {
	for i, c := range p {
		fmt.Printf("connection %d: peer %d, opened=%v, next offset %d\n", i, c.st.sid, c.st.opens, c.head)
		for off := c.head; off < c.st.end(); off++ {
			fmt.Printf("\t%+v\n", *c.st.at(off))
		}
	}
}
