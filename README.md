# ZooKeeper for Ellsberg

This is the Apache ZooKeeper used in the evaluation of *Ellsberg* (NSDI '25).
The source in [`zookeeper/`](zookeeper) is Apache ZooKeeper `3.9.1-SNAPSHOT`
(imported in August 2023) with changes that report the Zab and leader-election
messages each server sends and receives, and the client requests and replies it
handles, either to the simulator or to log files. ZooKeeper's own README is
[`zookeeper/README.md`](zookeeper/README.md).

## Build

Requires a JDK (the experiments used OpenJDK 17) and Maven.

```sh
cd zookeeper
mvn clean install -DskipTests
```

This produces
`zookeeper-assembly/target/apache-zookeeper-3.9.1-SNAPSHOT-bin.tar.gz`.

## Run

Unpack the binary distribution on each server and start it with the
environment variables described below. The configurations used in the
experiments are in `zookeeper/conf/` (3 servers) and `zookeeper/conf/conf5/`
(5 servers); they expect the servers at `10.10.1.1`, `10.10.1.2`, ... and
`dataDir=/mydata/zookeeper`, which must contain a `myid` file with the server
number.

```sh
cd zookeeper/zookeeper-assembly/target
tar zxf apache-zookeeper-3.9.1-SNAPSHOT-bin.tar.gz
cd apache-zookeeper-3.9.1-SNAPSHOT-bin
echo $IID > /mydata/zookeeper/myid
SIMMODE=FILE IID=$IID ./bin/zkServer.sh --config conf start-foreground
```

Currently only clients connected to the leader are supported: a follower
forwards client requests to the leader as `REQUEST` packets, which the leader
reports but the checker does not support.

## Configuration

Message reporting is configured through environment variables, read at
startup (`zookeeper-server/.../server/SimConn.java`).

| Variable | Values | Default | Meaning |
|---|---|---|---|
| `SIMMODE` | `NONE`, `ASYNC`, `FILE` | `NONE` | Where messages are reported (below). `SYNC` is not implemented. |
| `SIMCONNTYPE` | `PIPE` | `PIPE` | How to connect to the simulator (`ASYNC`). Only named pipes are supported. |
| `IID` | a number, unique per server | — | Instance ID; used in pipe and log file names. Required unless `SIMMODE` is `NONE`. |

`SIMMODE`:

- `NONE`: no reporting; ZooKeeper with the changes listed under
  [Deviations from ZooKeeper](#deviations-from-zookeeper).
- `ASYNC`: send each message to the simulator through a named pipe. Messages
  are queued and written by a separate thread.
- `FILE`: write messages to log files; no simulator.

### Hard-coded paths

Simulator connection (`ASYNC`): the server writes to the named pipe
`/tmp/tmpPipe_c_to_s.<IID>.pipe` and opens `/tmp/tmpPipe_s_to_c.<IID>.pipe`
(which it does not read). The simulator must create both pipes before the
server starts.

Log files (`FILE`) go to `/tmp/log/log<YYYYMMDD-HHMMSS>/` (the server's start
time), in one subdirectory per 1-second time slot. A time slot is named after
its start on the JVM's monotonic clock (`System.nanoTime()`), so the names look
like dates in 1970. Each server writes several streams, each in its own files
`messages.<IID>.<stream>.data` and `.meta`:

| Stream | Server | Contents |
|---|---|---|
| `simConn` | all | leader election, and messages not listed below |
| `handler.<sid>` | leader | messages exchanged with follower `<sid>` |
| `ack` | follower | ACKs sent to the leader |
| `recvreq` | leader | client requests received |
| `sendresp` | leader | replies sent to clients |

The `.meta` file has two lines: the cumulative number of messages in the
stream before and after this time slot. A time slot is written once the next
one starts, so the newest slot of each stream is lost if the server is killed.

### Message format

Records are serialized with ZooKeeper's jute binary format (big-endian). Each
record is an `int` record kind (`1` bytes, `2` quorum packet, `3` client
request/reply header) followed by the timestamp (`System.nanoTime()`), whether
the server sent (`true`) or received the message, the peer's server ID (or the
client session ID), the message type, and the message. In `ASYNC` mode each
record is preceded by an 8-byte length.

What is reported:

- Leader election notifications (sent and received). The checker parses them
  but does not check them.
- A TCP connection event when a follower connects to the leader.
- Zab packets: `FOLLOWERINFO`, `LEADERINFO`, `ACKEPOCH`, `NEWLEADER`, `ACK`
  (including the ACK of `NEWLEADER`), `UPTODATE`, `PROPOSAL`, `COMMIT`, and
  the synchronization packets (`DIFF`, `TRUNC`, and the proposals and commits
  sent during synchronization). The leader reports all synchronization
  packets for a follower as one record when it sends `NEWLEADER`.
- Client `create`, `setData`, `createSession` and `closeSession` requests
  received by the leader, and the replies to them.

Not reported: reads (ZooKeeper serves them locally), `PING`, other client
operations (`delete`, `multi`, `setACL`, `reconfig`, ...), and observers. Snapshot-based synchronization (`SNAP`) and
reconfiguration are not supported by the checker; the experiments start from
empty data directories.

## Deviations from ZooKeeper

**Behavior**

| Where | Change |
|---|---|
| `quorum/Follower.java`, `quorum/Learner.java` | A new `QuorumPacket` is allocated for every packet read (upstream reuses one), because reported packets are serialized later by the reporting thread. |
| `bin/zkServer.sh` | `start-foreground` runs the JVM with `-Xms8g -Xmx16g`. |
| `conf/zoo.cfg`, `conf/conf5/` | Experiment configurations: servers at `10.10.1.x`, `dataDir=/mydata/zookeeper`, `maxClientCnxns=1000`. |

**Reporting**

| Where | Change |
|---|---|
| `server/SimConn.java` | Simulator connection, log files and record format (new). |
| `quorum/QuorumPeerMain.java`, `quorum/QuorumPeer.java` | Create the reporting streams for each server role. |
| `quorum/QuorumCnxManager.java` | Report leader election notifications. |
| `quorum/Leader.java`, `quorum/LearnerHandler.java`, `quorum/Learner.java`, `quorum/Follower.java`, `quorum/SendAckRequestProcessor.java` | Report Zab packets and TCP connection events. In `Leader.java`, the packet type constants are made public. |
| `server/PrepRequestProcessor.java`, `server/FinalRequestProcessor.java` | Report client write requests and replies on the leader. |
| `server/ZooKeeperServer.java`, `quorum/LeaderZooKeeperServer.java`, `quorum/LearnerMaster.java` | Hold and shut down the reporting streams. |
