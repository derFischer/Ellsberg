//go:build etcd

package protocol

import "fmt"

// debug function
func dumpCandidate(c *Candidate) {
	dumpPending(c.P)
	fmt.Printf("STATE: %+v\n", c.S.State)
	fmt.Printf("TERM: %+v\n", c.S.CurrentTerm)
	fmt.Printf("VOTEDFOR: %+v\n", c.S.VotedFor)
	fmt.Printf("VOTES: %+v\n", c.S.Votes)
	fmt.Printf("LOG: last index: %+v, content: %+v\n", c.S.GetLastCommandIndex(), c.S.Log)
	fmt.Printf("COMMITINDEX: %+v\n", c.S.CommitIndex)
	fmt.Printf("MMATCHINDEX: %+v\n\n\n", c.S.MmatchIndex)
	fmt.Printf("QUORUM: %+v, %+v\n\n\n", c.S.Quorum[0], c.S.Quorum[1])
	fmt.Printf("CONNECTIONS: %+v\n", c.S.Connections)
	fmt.Printf("CLIENT_REQ_BUFFER: %+v\n", *(c.X))
	fmt.Printf("\n\n\n\n")
}
