# Ellsberg

Ellsberg (NSDI '25) checks a running distributed protocol implementation
against its specification. Each server records the messages it sends and
receives (its trace); Ellsberg replays one server's trace and reports a
violation as soon as the server sends a message that no execution of the
specification, consistent with the messages received so far, could send.

This branch is the checker. The other branches of this repository are:

| Branch           | Contents                                                           |
|------------------|--------------------------------------------------------------------|
| `ellsberg-etcd`  | etcd instrumented to record traces                                 |
| `ellsberg-redis` | RedisRaft instrumented to record traces                            |
| `ellsberg-zk`    | ZooKeeper instrumented to record traces                            |
| `asap_z3`        | Z3 checks that the `apply_asap?` rules of the specifications are safe |

## Layout

| Directory      | Contents |
|----------------|----------|
| `engine/`      | The protocol-independent checker: the candidate states and the search for the states that can send each outgoing message (`find_reachable` in the paper). |
| `network/`     | Pending messages: `Unordered` (Raft: any message can be delivered next) and `FIFO` (ZooKeeper: in order per TCP connection). |
| `raft/`        | Raft (etcd and RedisRaft): the specification (`spec/`), its connection to the engine and the trace driver (`protocol/`), trace parsers (`interpreter/`). |
| `zab/`         | ZooKeeper (Zab), organized the same way. |
| `comm/`        | Trace readers. |
| `statistics/`  | Optional per-message statistics. |

## Specifications

`raft/spec/` and `zab/spec/` hold the specifications, with the functions of
the paper (Section 3.2) in these files. In `raft/spec/`, files ending in
`_etcd.go` or `_redis.go` are specific to etcd or RedisRaft (selected by the
build tag); the others are shared.

| Function                  | Raft (`raft/spec/`)                       | ZooKeeper (`zab/spec/`)  |
|---------------------------|-------------------------------------------|--------------------------|
| State, initial state      | `state*.go`, `init_*.go`                  | `state.go`, `init.go`    |
| `apply`                   | `apply*.go`                               | `apply.go`               |
| `equal`                   | `equal*.go`                               | `equal.go`               |
| `infer_inducing`          | `inference.go`, `match*.go`               | `inference.go`, `match.go` |
| `reachable`               | `reachable.go`                            | `reachable.go`           |
| `apply_asap?`             | `asap.go`                                 | `asap.go`                |
| `lookahead_type`          | `look_ahead.go`                           | `look_ahead.go`          |
| Helpers                   | `gc.go`, `helper.go`                      | `gc.go`, `helper.go`     |

### Specification sizes

Since the paper, we have refactored the code. The current sizes of the
specifications, in lines of code (non-blank, non-comment) of the files above,
are below; the baselines are the Raft specification of Microsoft CCF (for etcd
and RedisRaft) and an implementation-derived TLA+ specification of ZooKeeper.

| Function          | etcd | RedisRaft | ZooKeeper |
|-------------------|-----:|----------:|----------:|
| State             |   45 |        47 |       102 |
| Initial state     |   16 |        14 |         6 |
| `apply`           |  368 |       323 |       433 |
| `equal`           |   64 |        64 |        73 |
| `infer_inducing`  |  314 |       287 |       246 |
| `reachable`       |   36 |        36 |        64 |
| `apply_asap?`     |   47 |        47 |        28 |
| `lookahead_type`  |    9 |         9 |         6 |
| Helpers           |  162 |       162 |        52 |
| **Total**         | **1061** | **989** | **1010** |
| Baseline          | 1503 |      1503 |      1615 |

## Building

The Raft trace parser uses etcd's message types, taken from the
`ellsberg-etcd` branch, which `go.mod` expects next to this repository as
`../etcd`:

```shell
git clone --single-branch -b main git@github.com:derFischer/Ellsberg.git Ellsberg
git clone --single-branch -b ellsberg-etcd git@github.com:derFischer/Ellsberg.git etcd
cd Ellsberg
go build -tags etcd  -o ellsberg-etcd .
go build -tags redis -o ellsberg-redis .
go build -tags zk    -o ellsberg-zk .
```

(Go 1.21 or later.)

## Checking a trace

Run the instrumented servers with `SIMMODE=FILE`: each server writes its trace
to a directory `log<time>` (see the README of each system's branch). Then
check one server's trace:

```shell
# etcd: server 2 of a 3-server cluster
SIMMODE=FILE IID=2 ./ellsberg-etcd  -a etcd  -i 2 --ids 1,2,3 --logdir <trace dir> -n <slots>
# RedisRaft
SIMMODE=FILE IID=2 ./ellsberg-redis -a redis -i 2 --ids 1,2,3 --logdir <trace dir> -n <slots>
# ZooKeeper
SIMMODE=FILE IID=2 ./ellsberg-zk    -a zk    -i 2 --ids 1,2,3 --logdir <trace dir> -n <slots>
```

A trace is written in time slots: one file per slot for etcd and RedisRaft
(`messages.*.data`), one directory per slot for ZooKeeper. The checker waits
for a slot to be complete before reading it, so `-n` (the number of slots to
check) must be lower than the number of slots written; for a finished trace,
use the number of files minus 2 (etcd, RedisRaft) or the number of
directories minus 1 (ZooKeeper).

The checker prints a line per slot (`processed N(sent+received) took T`). It
ends with `Reached LastMsgIdx` when the whole range is consistent with the
specification. Otherwise it prints the candidate states, then `No valid
states for this message` with the offending message, and exits with an
error.

Other options:

- `-s --exprname <name>`: record per-message statistics in `<trace dir>/<name>/`.
- `--asapmode off`, `--mergemode nomerge|exact|functional`, `-f`/`-t`:
  disable the optimizations (applying messages as soon as possible, merging
  equivalent states), for evaluating them.
