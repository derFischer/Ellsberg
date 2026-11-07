# etcd for Ellsberg

This is the etcd used in the evaluation of *Ellsberg* (NSDI '25). It is upstream
etcd at commit `772dfbfe3` (2020-07-20, between v3.4 and v3.5) with two kinds of
changes:

1. **Message reporting.** Each node reports the Raft messages it sends and
   receives, and the client requests and replies it handles, either to the
   simulator or to log files.
2. **Spec alignment.** A few Raft/etcd behaviors are changed so that the
   implementation follows the Raft specification used by the checker (for
   example, heartbeats are sent as empty AppendEntries).

The original etcd README is kept in [README-etcd.md](README-etcd.md).

## Build

Requires Go (the module declares Go 1.14; tested with Go 1.21).

```sh
go build -o bin/etcd .
```

## Run

Start each member as usual for etcd, adding the environment variables described
below. A 3-member cluster on one machine, writing message logs to files:

```sh
CLUSTER=n1=http://127.0.0.1:22001,n2=http://127.0.0.1:22002,n3=http://127.0.0.1:22003
for i in 1 2 3; do
  SIMMODE=FILE IID=$i ./bin/etcd --name n$i --data-dir data/n$i \
    --listen-peer-urls http://127.0.0.1:2200$i --initial-advertise-peer-urls http://127.0.0.1:2200$i \
    --listen-client-urls http://127.0.0.1:2300$i --advertise-client-urls http://127.0.0.1:2300$i \
    --initial-cluster $CLUSTER --initial-cluster-state new &
done
```

Clients must send requests to the leader; other members reply
`etcdserver: not leader` (see [Deviations from etcd](#deviations-from-etcd)).

## Configuration

Message reporting is configured through environment variables, read once at
startup (`comm/connection.go`).

| Variable | Values | Default | Meaning |
|---|---|---|---|
| `SIMMODE` | `NONE`, `ASYNC`, `SYNC`, `FILE`, `SYNCANDFILE` | `ASYNC` | Where messages are reported (below). |
| `SIMCONNTYPE` | `PIPE`, `UNIX`, `TCP` | `PIPE` | How to connect to the simulator (`ASYNC`, `SYNC`, `SYNCANDFILE`). |
| `IID` | any string, unique per member | empty | Instance ID; used in pipe, socket and log file names. |

`SIMMODE`:

- `NONE`: no reporting; plain etcd plus the spec-alignment changes.
- `ASYNC`: send each message to the simulator without waiting.
- `SYNC`: send each message to the simulator and block until the simulator
  acknowledges it.
- `FILE`: write messages to log files; no simulator.
- `SYNCANDFILE`: `SYNC` and `FILE` together.

In the simulator modes, if the simulator cannot be reached at startup, the
member prints `Warning: cannot connect to simulator!` and runs without
reporting.

### Hard-coded paths and addresses

Simulator connection, by `SIMCONNTYPE`:

| `SIMCONNTYPE` | Endpoint |
|---|---|
| `PIPE` | Named pipes `/tmp/tmpPipe_c_to_s.<IID>.pipe` (etcd → simulator) and `/tmp/tmpPipe_s_to_c.<IID>.pipe` (simulator → etcd). The simulator must create them. |
| `UNIX` | Unix socket `/tmp/server.<IID>.sock` |
| `TCP` | `127.0.0.1:9090` |

On connect, a member sends its own ID followed by a JSON list of all cluster member IDs.

Log files (`FILE`, `SYNCANDFILE`) go to `/tmp/log/log<YYYYMMDD-HHMMSS>/`, named
after the member's start time:

- `cluster.meta`: the member's own ID, then one line per cluster member ID.
- `messages.<IID>.<time>.data`: the reported messages, in the same binary
  format that is sent to the simulator.
- `messages.<IID>.<time>.meta`: two lines, the number of messages written when
  timing started and the number at the end of the interval.

After a 1-second warm-up, the member starts a new `.data`/`.meta` pair every
second and prints the message counts to stdout.

## Deviations from etcd

The changes below are marked with `Ellsberg:` comments in the code.

**Spec alignment (Raft)**

| Where | Change |
|---|---|
| `raft/raft.go` `sendHeartbeat` | Heartbeats are sent as empty `MsgApp` (prev index/term = leader's last log entry, full commit index). The ReadIndex context travels in `Context`. |
| `raft/raft.go` `stepLeader` | `MsgAppResp` also does what upstream does on `MsgHeartbeatResp`: clears `ProbeSent`, frees an inflight slot, and counts ReadIndex acks. |
| `raft/raft.go` `maybeSendAppend` | A `MsgApp` carries all entries from `Next` to the end of the leader's log (no `maxMsgSize` limit). |
| `raft/raft.go` `handleAppendEntries` | A `MsgApp` whose previous index is below the commit index is not discarded: the entries beyond the commit index are appended. If it carries nothing new, the reply has match index = previous index + number of entries, as in the spec. |
| `raft/raft.go` `Step` | The `MsgAppResp` sent in reply to a lower-term `MsgApp` is marked `Reject`. |
| `etcdserver/raft.go` `processMessages` | Every `MsgApp` gets its own `MsgAppResp`; upstream keeps only the last one per batch. Only responses carrying a ReadIndex context are still deduplicated. |
| `etcdserver/api/rafthttp/msgappv2_codec.go` | `MsgApp` with a `Context` always uses the full encoding, so the context is not dropped. |

No change was needed for the leader's own match index. As in the Raft TLA+
specification (Ongaro, `AdvanceCommitIndex`), where the leader counts itself
toward the commit quorum for every entry in its log as soon as it appends the
entry, etcd's leader updates its own progress immediately when it appends an
entry (`raft/raft.go` `appendEntry`: `r.prs.Progress[r.id].MaybeUpdate(li)`),
before the entry is persisted.

**Leader-only clients**

| Where | Change |
|---|---|
| `etcdserver/v3_server.go` `Range`, `processInternalRaftRequestOnce` | Reads and writes are accepted only on the leader; followers return `ErrNotLeader`. |
| `raft/raft.go` `stepFollower`, `stepCandidate` | `MsgReadIndex` is rejected on followers and candidates instead of being forwarded or ignored. |
| `raft/raft.go` `stepFollower` | Followers forward proposals to the leader only while the log is within the bootstrap prefix (2N+2 entries for N members), so that each member can publish its attributes at startup. Afterwards proposals on followers are dropped. |

**Other**

| Where | Change |
|---|---|
| `raft/raft.go` `send`, `etcdserver/server.go` `Process`, `configure`, `etcdserver/v3_server.go` | Reporting hooks for Raft messages and client requests/replies (packages `comm`, `interpreter`, `messagepb`). |
| `raft/node.go` | `Node` interface gains `IsLeader()` and `SetSlConn()`. |
| `etcdserver/v3_server.go` `raftRequestOnce` | Upstream's slow-request trace logging is disabled. |
