# Ellsberg: checking the `apply_asap?` rules with Z3

Ellsberg applies some received messages as soon as they arrive
(`apply_asap?`, paper Section 3.3.2) instead of exploring every order in which
they could be processed. These scripts use Z3 to check that the rules in the
Raft and ZooKeeper specifications are safe: they satisfy the four conditions of
Appendix B ("Proving apply ASAP correct"), which imply that applying a message
early does not lose any behavior.

The scripts number the conditions differently from the paper:

| Script      | Paper condition                         |
|-------------|-----------------------------------------|
| statement 1 | Order preserving (Condition 1)          |
| statement 2 | Reorder safety (Condition 4)            |
| statement 3 | Expansion (Condition 3)                 |
| statement 4 | Constraints preserving (Condition 2)    |

## Setup

```shell
python3 -m venv venv
venv/bin/pip install z3-solver rich
```

## Raft

`raft/z3-raft.py` models the Raft specification for three servers (one entry
per AppendEntries request). Its events are RequestVote requests and responses,
AppendEntries requests and responses, client requests, `checkLeader` and
`timeout`.

```shell
cd raft && ../venv/bin/python z3-raft.py
```

It checks the rule of each message type (AppendEntries requests and responses,
RequestVote requests and responses), listed in `rules` near the end of the
file, with statements 2-4. Expected output:

```
states constraints are satisfied
statement 1 is satisfied
AppendEntriesResponse:
statement 2 is satisfied
statement 3 is satisfied
statement 4 is satisfied
RequestVoteResponse:
statement 2 is satisfied
statement 3 is satisfied
statement 4 is satisfied
AppendEntries:
statement 2 is satisfied
statement 3 is satisfied
statement 4 is satisfied
RequestVote:
statement 2 is satisfied
statement 3 is satisfied
statement 4 is satisfied
```

## ZooKeeper

`zk/zk.py` models the Zab specification for three servers and three log
entries, and checks the ACK and ACKLD rules. `state_constraints` and
`message_constraints` restrict the check to states and messages that can occur;
`can_swap` reorders a message only with messages from other senders (each
connection is FIFO) and not with local events (see the comment there).

```shell
cd zk && ../venv/bin/python zk.py
```

It takes about 25 seconds. Expected output:

```
Valid message constraints are satisfied.
State constraints are valid.
Statement 1 is satisfied
Statement 2 for ACK is satisfied
Statement 3 for ACK is satisfied
statement 4 is satisfied
Statement 2 for ACKLD is satisfied
Statement 3 for ACKLD is satisfied
statement 4 is satisfied
```
