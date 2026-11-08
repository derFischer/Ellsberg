package network

import "reflect"

// Unordered is an unordered network, such as the one Raft assumes: any
// pending message can be delivered next. Equal messages are kept once, with a
// count.
type Unordered[M any] []entry[M]

type entry[M any] struct {
	msg   *M
	count int
}

// UnorderedNet is the Network of Unordered pending messages; it keeps no
// messages itself.
type UnorderedNet[M any] struct{}

func (UnorderedNet[M]) Receive(m *M) Arrival[M] {
	return Arrival[M]{Msg: m}
}

func (UnorderedNet[M]) Trim([]Unordered[M]) {}

func (p Unordered[M]) Received(a Arrival[M]) Unordered[M] {
	for i := range p {
		if reflect.DeepEqual(*p[i].msg, *a.Msg) {
			q := make(Unordered[M], len(p))
			copy(q, p)
			q[i].count++
			return q
		}
	}
	q := make(Unordered[M], len(p), len(p)+1)
	copy(q, p)
	return append(q, entry[M]{msg: a.Msg, count: 1})
}

func (p Unordered[M]) Len() int {
	total := 0
	for _, e := range p {
		total += e.count
	}
	return total
}

// Distinct returns the number of distinct pending messages.
func (p Unordered[M]) Distinct() int {
	return len(p)
}

func (p Unordered[M]) Slots() int {
	return len(p)
}

func (p Unordered[M]) Next(i int) *M {
	return p[i].msg
}

func (p Unordered[M]) Find(m *M) int {
	for i, e := range p {
		if e.msg == m {
			return i
		}
	}
	return -1
}

func (p Unordered[M]) Deliver(i int) Unordered[M] {
	if p[i].count > 1 {
		q := make(Unordered[M], len(p))
		copy(q, p)
		q[i].count--
		return q
	}
	return p.without(i)
}

func (p Unordered[M]) without(i int) Unordered[M] {
	q := make(Unordered[M], 0, len(p)-1)
	q = append(q, p[:i]...)
	return append(q, p[i+1:]...)
}

// Covers reports whether o is a sub-multiset of p: messages that are pending
// need not be delivered, so p allows everything o allows.
func (p Unordered[M]) Covers(o Unordered[M]) bool {
	if len(o) > len(p) {
		return false
	}
	if len(p) > 64 {
		return p.coversSlow(o)
	}
	var used uint64 // entries of p already matched
	for _, e := range o {
		found := false
		for i := range p {
			if used&(1<<i) == 0 && p[i].msg == e.msg && p[i].count >= e.count {
				found = true
				used |= 1 << i
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (p Unordered[M]) coversSlow(o Unordered[M]) bool {
	rest := make(Unordered[M], len(p))
	copy(rest, p)
	for _, e := range o {
		found := false
		for i := range rest {
			if rest[i].msg == e.msg && rest[i].count >= e.count {
				found = true
				rest = append(rest[:i], rest[i+1:]...)
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Saturate delivers the first message apply accepts (all its copies), then
// starts over, until apply accepts none.
func (p Unordered[M]) Saturate(apply func(*M) bool) (Unordered[M], int, int) {
	delivered, tried := 0, 0
	for progress := true; progress; {
		progress = false
		for i, e := range p {
			tried += e.count
			if apply(e.msg) {
				delivered += e.count
				p = p.without(i)
				progress = true
				break
			}
		}
	}
	return p, delivered, tried
}

func (p Unordered[M]) Each(f func(*M) bool) {
	for _, e := range p {
		if !f(e.msg) {
			return
		}
	}
}

// Remove returns p without the messages drop accepts.
func (p Unordered[M]) Remove(drop func(*M) bool) Unordered[M] {
	var q Unordered[M]
	for i, e := range p {
		if drop(e.msg) {
			if q == nil {
				q = append(make(Unordered[M], 0, len(p)), p[:i]...)
			}
			continue
		}
		if q != nil {
			q = append(q, e)
		}
	}
	if q == nil {
		return p
	}
	return q
}

// Count returns how many copies of the message in slot i are pending.
func (p Unordered[M]) Count(i int) int {
	return p[i].count
}
