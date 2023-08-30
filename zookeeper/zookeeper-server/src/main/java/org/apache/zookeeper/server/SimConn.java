package org.apache.zookeeper.server;

import org.apache.jute.*;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class SimConn {
    public static class SimConnMessage implements Record {
        final static byte[] emptyByteArray = new byte[]{};
        boolean isSend;
        long peerId;
        // not | qp
        String type;
        byte[] recv;
        byte[] send;

        protected static final Logger LOG = LoggerFactory.getLogger(SimConnMessage.class);

        static public SimConnMessage createForClientReq(long peerId, TxnHeader hdr) {
            SimConnMessage m = new SimConnMessage();
            m.isSend = false;
            m.peerId = peerId;
            m.type = "CLIENTREQ";
            m.send = emptyByteArray;
            m.recv = m.quorumPacketToBytes(hdr);
            return m;
        }
        static public SimConnMessage createForClientResp(long peerId, TxnHeader hdr) {
            SimConnMessage m = new SimConnMessage();
            m.isSend = true;
            m.peerId = peerId;
            m.type = "CLIENTRESP";
            m.send = m.quorumPacketToBytes(hdr);
            m.recv = emptyByteArray;
            return m;
        }

        public SimConnMessage() {}

        public SimConnMessage(long peerId, String type, byte[] recv, byte[] send) {
            assert (recv != null || send != null);
            this.isSend = send != null;
            this.peerId = peerId;
            this.type = type;
            this.recv = recv != null ? recv : emptyByteArray;
            this.send = send != null ? send : emptyByteArray;
        }

        byte[] quorumPacketToBytes(Record qp) {
            ByteArrayOutputStream s = new ByteArrayOutputStream();
            try {
                if (qp != null) {
                    DataOutputStream ds = new DataOutputStream(s);
                    BinaryOutputArchive outputArchive = new BinaryOutputArchive(ds);
                    qp.serialize(outputArchive, "qp");
                    ds.flush();
                    return s.toByteArray();
                } else {
                    return emptyByteArray;
                }
            } catch (java.io.IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }
            return emptyByteArray;
        }

        public SimConnMessage(long peerId, String type, QuorumPacket recv, QuorumPacket send) {
            assert (recv != null || send != null);
            this.isSend = send != null;
            this.peerId = peerId;
            this.type = type;
            ByteArrayOutputStream s = new ByteArrayOutputStream();
            this.recv = quorumPacketToBytes(recv);
            this.send = quorumPacketToBytes(send);
        }

        public SimConnMessage(long peerId, QuorumPacket recv, QuorumPacket send) {
            assert (recv != null || send != null);
            this.isSend = send != null;
            this.peerId = peerId;
            if (recv == null && send != null) {
                this.type = Leader.getPacketType(send.getType());
            } else if (recv != null && send == null) {
                this.type = Leader.getPacketType(recv.getType());
            } else {
                this.type = Leader.getPacketType(recv.getType()) + "," + Leader.getPacketType(send.getType());
            }
            ByteArrayOutputStream s = new ByteArrayOutputStream();
            this.recv = quorumPacketToBytes(recv);
            this.send = quorumPacketToBytes(send);
        }

        public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(this,tag);
            a_.writeBool(isSend, "isSend");
            a_.writeLong(peerId, "peerId");
            a_.writeString(type, "type");
            a_.writeBuffer(recv, "recv");
            a_.writeBuffer(send, "send");
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive archive, String tag) throws java.io.IOException {
            LOG.error("deserialize not implemented");
            System.exit(1);
        }

        public long length() {
            return 1 + 8 + (4+type.length()) + (4+recv.length) + (4+send.length);
        }

        public String toString() {
            return String.format("isSend=%b, type=%s\n\trecv: %s\n\tsend: %s\n",
                    isSend, type, Arrays.toString(recv), Arrays.toString(send));
        }
    }
    private enum MODE {
        NONE,
        ASYNC,
        SYNC,
        FILE
    }

    private enum CONNTYPE {
        TCP,
        UNIX,
        PIPE
    }


    MODE mode;
    CONNTYPE conntype;


    // Just in case we may need it.
    private long iid;
    private long myid;

    // NOTE: Don't directly use inConn/outConn, but use inArchive/outArchive
    private InputStream inConn;
    private OutputStream outConn;

    private InputArchive inArchive;
    private OutputArchive outArchive;

    final private ReentrantLock lock;

    // TODO: FILE MODE related

    protected static final Logger LOG = LoggerFactory.getLogger(SimConn.class);

    public static class AsyncSender extends Thread {
        // Note that BlockingQueue is thread-safe, we don't need additional lock.
        private final BlockingQueue<SimConnMessage> sendQueue;
        final private OutputStream outConn;
        final private OutputArchive outArchive;

        /*
         * Note that the `outConn` could even be a file stream
         */
        AsyncSender(OutputStream outConn) {
            this.outConn = outConn;
            this.outArchive = BinaryOutputArchive.getArchive(outConn);
            this.sendQueue = new LinkedBlockingQueue<>();
        }

        public void run() {
            while (true) {
                try {
                    SimConnMessage m = sendQueue.take();
                    outArchive.writeLong(m.length(), "length");
                    m.serialize(outArchive, "packet");
                    outConn.flush();
                } catch (java.io.IOException | InterruptedException e) {
                    LOG.info(e.toString());
                    System.exit(1);
                }
            }
        }

        public void Put(SimConnMessage m) {
            try {
                sendQueue.put(m);
            } catch (InterruptedException e) {
                LOG.info(e.toString());
                System.exit(1);
            }
        }
    }

    private AsyncSender asyncSender;
    public long msgCount;
    final private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private String logDir;

    public SimConn() {
        this.lock = new ReentrantLock();
    }

    public void setupEnv() {
        final String simmodeStr = System.getenv("SIMMODE");
        if (simmodeStr == null) {
            this.mode = MODE.ASYNC;
        } else if (simmodeStr.equals("NONE")) {
            this.mode = MODE.NONE;
        } else if (simmodeStr.equals("ASYNC")) {
            this.mode = MODE.ASYNC;
        } else if (simmodeStr.equals("SYNC")) {
            this.mode = MODE.SYNC;
        } else if (simmodeStr.equals("FILE")) {
            this.mode = MODE.FILE;
        } else {
            LOG.error("Unexpected simmode: " + simmodeStr);
            System.exit(1);
        }
        
        final String simconntypeStr = System.getenv("SIMCONNTYPE");
        if (simconntypeStr == null) {
            this.conntype = CONNTYPE.PIPE;
        } else if (simconntypeStr.equals("TCP")) {
            this.conntype = CONNTYPE.TCP;
        } else if (simconntypeStr.equals("UNIX")) {
            this.conntype = CONNTYPE.UNIX;
        } else if (simconntypeStr.equals("PIPE")) {
            this.conntype = CONNTYPE.PIPE;
        } else {
            LOG.error("Unexpected simconntype: " + simconntypeStr);
        }

        if (this.mode != MODE.NONE) {
            this.iid = Long.parseLong(System.getenv("IID"));
        }
    }

    public void init(final long myId) {
        this.setupEnv();
        this.myid = myId;
        LOG.info("SimConn setup: mode={}, conntype={}, iid={}, myid={}", mode, conntype, iid, myid);

        if (mode == MODE.NONE) {
            return;
        }

        if (mode == MODE.FILE) {
            LOG.error("FILE mode is not implemented now.");
            msgCount = 0;
            String formatTime = dateFormatter.format(ZonedDateTime.now());
            logDir = "/mydata/log" + formatTime;
            new File(logDir).mkdirs();

            System.exit(1);
        } else {
            if (conntype == CONNTYPE.PIPE) {
                // TODO: uncomment
                String stocPath = "/tmp/tmpPipe_s_to_c." + iid + ".pipe";
                String ctosPath = "/tmp/tmpPipe_c_to_s." + iid + ".pipe";
                try {
                    outConn = Files.newOutputStream(Paths.get(ctosPath));
                    outArchive = BinaryOutputArchive.getArchive(outConn);
                    inConn = Files.newInputStream(Paths.get(stocPath));
                    inArchive = BinaryInputArchive.getArchive(inConn);
                } catch (Exception e) {
                    LOG.error(e.toString());
                    System.exit(1);
                }
                LOG.info("\033[1;31mSimConn Initialized. Connected to {} and {}\033[0m", stocPath, ctosPath);
            } else {
                LOG.error("Only PIPE conntype is implemented");
            }
        }

        // TODO: implement for SYNC and FILE
        if (this.mode == MODE.ASYNC) {
            asyncSender = new AsyncSender(outConn);
            asyncSender.start();
        }
    }

    public void print(String who, QuorumPacket qp) {
        if (mode != MODE.NONE) {
            try {
                if (!Leader.getPacketType(qp.getType()).equals("PING")) {
                    LOG.info(who + qp);
                }
            } catch(Exception e) {
                // catch any bug inside SimConn...
                LOG.error(e.toString());
                System.exit(1);
            }
        }
    }

    public void request(QuorumPacket qp) {
        if (mode != MODE.NONE) {
            if (!Leader.getPacketType(qp.getType()).equals("PING")) {
                LOG.info("[writePacket] " + qp);
            }
        }
    }

    public void request(SimConnMessage m) {
        if (mode != MODE.NONE) {
            if (mode == MODE.ASYNC) {
                try {
                    lock.lock();
                    asyncSender.Put(m);
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}



// ByteBuffer buffer = ByteBuffer.wrap(m.msgBytes);
// buffer.clear();
//
// int rstate = buffer.getInt();
// long rleader = buffer.getLong();
// long rzxid = buffer.getLong();
// long relectionEpoch = buffer.getLong();
// long rpeerepoch = buffer.getLong();
// int version = buffer.getInt();
//
// // State of peer that sent this message
// QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
// switch (rstate) {
//     case 0:
//         ackstate = QuorumPeer.ServerState.LOOKING;
//         break;
//     case 1:
//         ackstate = QuorumPeer.ServerState.FOLLOWING;
//         break;
//     case 2:
//         ackstate = QuorumPeer.ServerState.LEADING;
//         break;
//     case 3:
//         ackstate = QuorumPeer.ServerState.OBSERVING;
//         break;
//     default:
//         System.exit(1);
// }
// int configLength = buffer.getInt();
// byte[] b = new byte[configLength];
// buffer.get(b);
// String config = new String(b, UTF_8);
//
// LOG.info("[fle] leader={}, zxid={}, electionEpoch={}, state={}, sid={}, peerEpoch={}, version={}, config={}", rleader, rzxid, relectionEpoch, ackstate, -1, rpeerepoch, version, config);