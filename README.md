# RedisRaft for Ellsberg

This is the RedisRaft used in the evaluation of *Ellsberg* (NSDI '25). It is
upstream [RedisRaft](https://github.com/RedisLabs/redisraft) at commit
`49b292c` (2023-01-19), including its bundled Raft library (`deps/raft`), with
changes that report the Raft messages each node sends and receives, and the
client requests and replies it handles, either to the simulator or to log
files.

The original RedisRaft README is kept in [README-redisraft.md](README-redisraft.md).

## Build

Requires a C compiler, CMake, GNU make and GNU autotools.

```sh
mkdir build && cd build
cmake ..
make
```

`redisraft.so` is created in the project directory. The default build type is
`Release`.

RedisRaft also needs a Redis server built from source; the experiments used
Redis commit `cd58af4d7fe4b506899591b2846cfba61335c958`:

```sh
git clone https://github.com/redis/redis && cd redis
git checkout cd58af4d7fe4b506899591b2846cfba61335c958
make && make install
```

## Run

Start each member with the environment variables described below, then create
the cluster. A 3-member cluster on one machine, writing message logs to files:

```sh
for i in 1 2 3; do
  SIMMODE=FILE IID=$i redis-server --port 500$i --dbfilename raft$i.rdb \
    --loadmodule $PWD/redisraft.so \
    --raft.log-filename raftlog$i.db --raft.addr localhost:500$i --raft.id $i &
done
redis-cli -p 5001 RAFT.CLUSTER INIT
redis-cli -p 5002 RAFT.CLUSTER JOIN localhost:5001
redis-cli -p 5003 RAFT.CLUSTER JOIN localhost:5001
```

Clients send requests to the leader (member 1 after `INIT`). The checker
supports `SET` and `GET`.

## Configuration

Message reporting is configured through environment variables, read when the
Raft instance is created (`deps/raft/src/raft_forwarder.c`).

| Variable | Values | Default | Meaning |
|---|---|---|---|
| `SIMMODE` | `NONE`, `ASYNC`, `SYNC`, `FILE` | `NONE` | Where messages are reported (below). |
| `SIMCONNTYPE` | `UNIX` | `UNIX` | How to connect to the simulator. Only Unix domain sockets are supported. |
| `IID` | any string, unique per member | — | Instance ID; used in socket and log file names. Required unless `SIMMODE` is `NONE`. If `--raft.id` is not given, it is also used as the Raft node ID. |

`SIMMODE`:

- `NONE`: no reporting; RedisRaft with the changes listed under
  [Deviations from RedisRaft](#deviations-from-redisraft).
- `ASYNC`, `SYNC`: send each message to the simulator. Both behave the same:
  the member writes each message to the socket and does not wait for a reply.
- `FILE`: write messages to log files; no simulator.

Messages are reported on Redis's main thread; each write blocks until it
completes.

### Hard-coded paths

Simulator connection (`ASYNC`, `SYNC`): the member binds the Unix socket
`/tmp/client.<IID>.sock` and connects to `/tmp/server.<IID>.sock`, which the
simulator must create. If the connection fails, the member exits.

Log files (`FILE`) go to `/tmp/log/log<YYYYMMDD-HHMMSS>/`, named after the time
the Raft instance was created:

- `messages.<IID>.<time>.data`: the reported messages.
- `messages.<IID>.<time>.meta`: two lines, the cumulative number of messages
  written when the data file was opened and when it was closed.

The member starts a new `.data`/`.meta` pair every second. The `.meta` file is
written when its data file is closed, so the newest pair is incomplete while
the member runs, and is lost if the member is killed.

### Message format

Each record, in a file or on the socket, is an 8-byte length (native byte
order, little-endian on x86) followed by a RESP array: `[send, type, id, ...]`.

- `send`: `1` if the member sent the message, `0` if it received it.
- `type`: `1` client request, `2` successful client reply, `3` failed client
  reply, `4` Raft request (AppendEntries, RequestVote), `5` Raft response.
- `id`: the request ID for client requests and replies; the message ID for
  Raft messages (`0` where RedisRaft does not track one).
- The rest is the command or message. For lowercase `set` and `get`, only the
  command name is included, not the key and value.

What is reported:

| Message | Reported by |
|---|---|
| Client request (`RAFT <command>`) | the member receiving it |
| Client reply | the leader, before replying |
| `RAFT.NODE REMOVE` request and reply | the member receiving it (the leader) |
| AppendEntries | the sender (with the request IDs of its entries) |
| AppendEntries response | the sender (with the request it answers) and the receiver |
| RequestVote (not prevote) | the sender |
| RequestVote response (not prevote) | the sender and the receiver |

Reconfiguration: removing a member (`RAFT.NODE REMOVE`) is reported and
checked. Adding a member is how RedisRaft forms the cluster (`RAFT.CLUSTER
JOIN` adds each member, first as non-voting, then as voting), so the
`RAFT.NODE ADD` request and reply are not reported; the log entries it creates
are reported inside AppendEntries, and the checker accepts the entries of the
initial membership as a bootstrap prefix. Adding a member after the cluster
is formed is therefore not checked.

Not reported: snapshots, and error replies to clients (for example redirects
to the leader).

## Deviations from RedisRaft

**Behavior**

| Where | Change |
|---|---|
| `src/raft.c` `RaftLibraryInit` | `RAFT_CONFIG_AUTO_FLUSH` is `1` (upstream: `0`), to match the Raft TLA+ specification (Ongaro, `AdvanceCommitIndex`), in which the leader counts itself toward the commit quorum for every entry in its log as soon as it appends the entry. Upstream RedisRaft fsyncs new entries on a background thread and counts the leader's own log toward the commit only up to the fsynced index, so the leader's commit index (and the commit index it sends in AppendEntries) can lag behind the spec's. With auto-flush, each new entry is fsynced on the main thread and the leader's match index advances immediately; AppendEntries are also sent right away instead of being batched in `handleBeforeSleep`. |
| `CMakeLists.txt` | The default build type is `Release` (upstream: `RelWithDebInfo`). |
| `src/redisraft.c` `clusterInit` | Uses `IID` as the node ID when `--raft.id` is not given. |

**Reporting**

| Where | Change |
|---|---|
| `deps/raft/src/raft_forwarder.c`, `deps/raft/include/raft_forwarder.h` | Simulator connection and log files (new). |
| `src/interpreter.c`, `src/interpreter.h` | Encode messages for the simulator (new). |
| `src/raft.c`, `src/redisraft.c` | Reporting hooks for client requests and replies, AppendEntries, RequestVote and `RAFT.NODE REMOVE`. |
| `deps/raft/include/raft.h`, `deps/raft/src/raft_node.c` | Each log entry carries the request ID of the client request that created it. IDs are generated by the member that receives the request; they are not sent between members or stored in the log, so entries on followers and entries read back from disk have request ID `0`. |
| `deps/raft/src/raft_server.c`, `raft_server_properties.c`, `include/raft_private.h` | The Raft instance owns the forwarder. |
