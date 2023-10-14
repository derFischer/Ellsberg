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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class SimConn {
    public static boolean enabled = false;

    public static interface SimConnMessage extends Record {
        long length();
    }

    public static class TimeoutMessage implements SimConnMessage {
        @Override
        public void serialize(OutputArchive archive, String tag) throws IOException {}

        @Override
        public void deserialize(InputArchive archive, String tag) throws IOException {}
        public long length() { return -1; }
    }

    public static TimeoutMessage timeoutMessage = new TimeoutMessage();

    public static final int NOT = 0x10000000;
    public static final int CLIENTREQ = 0x10000001;
    public static final int CLIENTRESP = 0x10000002;
    public static final int CLIENTRO = 0x10000003;
    public static final int CLIENTRORESP = 0x10000004;

    public static class RecordSimConnMessage implements SimConnMessage {
        protected static final Logger LOG = LoggerFactory.getLogger(SimConnMessage.class);
        final static byte[] emptyByteArray = new byte[]{};
        boolean isSend;
        long peerId;
        // not | qp
        int type = -1;
        Record record;

        byte[] bytes;

        byte[] quorumPacketToBytes(Record qp) {
            if (qp == null) {
                return emptyByteArray;
            }
            ByteArrayOutputStream s = new ByteArrayOutputStream();
            try {
                DataOutputStream ds = new DataOutputStream(s);
                BinaryOutputArchive outputArchive = new BinaryOutputArchive(ds);
                qp.serialize(outputArchive, "qp");
                ds.flush();
                return s.toByteArray();
            } catch (java.io.IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }
            return null;
        }

        public RecordSimConnMessage() {}

        static public RecordSimConnMessage createForClientReq(long peerId, TxnHeader hdr, boolean isReadOnly) {
            RecordSimConnMessage m = new RecordSimConnMessage();
            if (!enabled) return m;
            m.isSend = false;
            m.peerId = peerId;
            if (!isReadOnly) {
                m.type = CLIENTREQ;
            } else {
                m.type = CLIENTRO;
            }
            m.record = hdr;
            return m;
        }
        static public RecordSimConnMessage createForClientResp(long peerId, TxnHeader hdr, boolean isReadOnly) {
            RecordSimConnMessage m = new RecordSimConnMessage();
            if (!enabled) return m;
            m.isSend = true;
            m.peerId = peerId;
            if (!isReadOnly) {
                m.type = CLIENTRESP;
            } else {
                m.type = CLIENTRORESP;
            }
            m.record = hdr;
            return m;
        }

        public RecordSimConnMessage(long peerId, boolean isSend, QuorumPacket qp) {
            if (!enabled) return;
            assert (qp != null);
            this.isSend = isSend;
            this.peerId = peerId;
            this.type = qp.getType();
            this.record = qp;
        }

        public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
            if (bytes == null) {
                bytes = quorumPacketToBytes(record);
            }
            a_.startRecord(this,tag);
            a_.writeBool(isSend, "");
            a_.writeLong(peerId, "");
            a_.writeInt(type, "");
            a_.writeBuffer(bytes, "");
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive archive, String tag) throws java.io.IOException {
            LOG.error("deserialize not implemented");
            System.exit(1);
        }

        public long length() {
            if (bytes == null) {
                bytes = quorumPacketToBytes(record);
            }
            return 1 + 8 + 4 + (4+bytes.length);
        }
    }

    public static class BytesSimConnMessage implements SimConnMessage {

        protected static final Logger LOG = LoggerFactory.getLogger(SimConnMessage.class);
        final static byte[] emptyByteArray = new byte[]{};
        boolean isSend;
        long peerId;
        // not | qp
        int recvType = -1;
        int sendType = -1;
        byte[] recv;
        byte[] send;

        public BytesSimConnMessage() {}

        public BytesSimConnMessage(long peerId, int type, byte[] recv, byte[] send) {
            if (!enabled) return;
            assert (recv != null || send != null);
            this.isSend = send != null;
            this.peerId = peerId;
            if (recv == null) {
                this.recv = emptyByteArray;
                this.recvType = -1;
            } else {
                this.recv = recv;
                this.recvType = type;
            }
            if (send == null) {
                this.send = emptyByteArray;
                this.sendType = -1;
            } else {
                this.send = send;
                this.sendType = type;
            }
        }

        public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(this,tag);
            a_.writeBool(isSend, "");
            a_.writeLong(peerId, "");
            a_.writeInt(recvType, "");
            a_.writeInt(sendType, "");
            a_.writeBuffer(recv, "");
            a_.writeBuffer(send, "");
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive archive, String tag) throws java.io.IOException {
            LOG.error("deserialize not implemented");
            System.exit(1);
        }

        public long length() {
            return 1 + 8 + 4 + 4 + (4+recv.length) + (4+send.length);
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
        protected final LinkedBlockingQueue<SimConnMessage> sendQueue;
        protected OutputStream outConn;
        protected OutputArchive outArchive;
        public long msgCount;

        AsyncSender() {
            this.sendQueue = new LinkedBlockingQueue<>(4096);
        }
        /*
         * Note that the `outConn` could even be a file stream
         */
        AsyncSender(OutputStream outConn) {
            this();
            this.outConn = outConn;
            updateOutArchive(outConn);
        }

        public void updateOutArchive(OutputStream outConn) {
            this.outConn =  outConn;
            this.outArchive = BinaryOutputArchive.getArchive(outConn);
        }

        public void run() {
            while (true) {
                try {
                    SimConnMessage m = sendQueue.take();
                    outArchive.writeLong(m.length(), "length");
                    m.serialize(outArchive, "packet");
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

        public void writeOne(SimConnMessage m) {
            msgCount++;
            try {
                outArchive.writeLong(m.length(), "length");
                m.serialize(outArchive, "packet");
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }

    public static class AsyncFileWriter extends AsyncSender {
        public long iid;
        public String logDir;
        public Path metaFilePath;

        final static private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
        AsyncFileWriter(long iid) {
            super();
            this.iid = iid;
            String formateTime = dateFormatter.format(ZonedDateTime.now());
            this.logDir = "/tmp/log/log" + formateTime;
            new File(logDir).mkdirs();
            changeFile();
        }

        public void changeFile() {
            String formateTime = dateFormatter.format(ZonedDateTime.now());
            Path path = Paths.get(this.logDir, String.format("messages.%s.%s.data", iid, formateTime));
            try {
                outConn = new BufferedOutputStream(Files.newOutputStream(path), 8192);
            } catch (IOException e) {
                LOG.info(e.toString());
                System.exit(1);
            }
            metaFilePath = Paths.get(this.logDir, String.format("messages.%s.%s.meta", iid, formateTime));
            updateOutArchive(outConn);
            LOG.info("use next filepath: {}", path);
        }

        @Override
        public void run() {
            Runnable putTimeout = () -> this.Put(timeoutMessage);
            TimerTask timeoutTask = new TimerTask() {
                public void run() {
                    putTimeout.run();
                }
            };

            Timer timer = new Timer();
            timer.schedule(timeoutTask, 30*1000L);
            long startMsgCount = 0;
            while (true) {
                try {
                    SimConnMessage m = sendQueue.take();
                    if (m == timeoutMessage) {
                        outConn.flush();
                        FileWriter metaWriter = new FileWriter(metaFilePath.toFile());
                        metaWriter.write(String.format("%d\n%d\n", startMsgCount, msgCount));
                        startMsgCount = msgCount;
                        metaWriter.close();
                        changeFile();
                        timer = new Timer();
                        timer.schedule(new TimerTask() {
                                public void run() {
                                    putTimeout.run();
                                }
                            },
                            60*1000L
                        );
                        continue;
                    }
                    msgCount++;
                    outArchive.writeLong(m.length(), "length");
                    m.serialize(outArchive, "packet");
                } catch (IOException | InterruptedException e) {
                    LOG.error(e.toString());
                    System.exit(1);
                }
            }
        }
    }

    private AsyncSender asyncSender;
    public long msgCount;

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
        enabled = this.mode != MODE.NONE;
    }

    public void init(final long myId) {
        this.setupEnv();
        this.myid = myId;
        LOG.info("SimConn setup: mode={}, conntype={}, iid={}, myid={}", mode, conntype, iid, myid);

        if (mode == MODE.NONE) {
            return;
        }

        if (mode != MODE.FILE) {
            if (conntype == CONNTYPE.PIPE) {
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
        switch (this.mode) {
        case ASYNC:
            asyncSender = new AsyncSender(outConn);
            asyncSender.start();
            break;
        case FILE:
            asyncSender = new AsyncFileWriter(iid);
            asyncSender.start();
            break;
        }
    }

    public void request(SimConnMessage m) {
        if (mode != MODE.NONE) {
            if (mode == MODE.ASYNC || mode == MODE.FILE) {
                asyncSender.Put(m);
                // try {
                //     lock.lock();
                //     asyncSender.writeOne(m);
                // } finally {
                //     lock.unlock();
                // }
            }
        }
    }

    public boolean enabled() {
        return this.mode != MODE.NONE;
    }
}
