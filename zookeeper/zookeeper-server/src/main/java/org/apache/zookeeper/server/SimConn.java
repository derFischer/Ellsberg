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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimConn {
    public static boolean enabled = false;
    final static public DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneId.systemDefault());
    static final public long initTimeMilli = System.currentTimeMillis();
    static final public long initTimeNano = System.nanoTime();

    public static class SimConnMessage implements Record {
        final public static int TIMEOUT_TYPE = 0;
        final public static int BYTES_TYPE = 1;
        final public static int QP_TYPE = 2;
        final public static int HDR_TYPE = 3;
        int simConnMessageType;
        long timestamp;

        public SimConnMessage() {
            simConnMessageType = -1;
        }

        public SimConnMessage(int simConnMessageType) {
            this.simConnMessageType = simConnMessageType;
        }
        public SimConnMessage(long ts, int simConnMessageType) {
            this.timestamp = ts;
            this.simConnMessageType = simConnMessageType;
        }

        @Override
        public void serialize(OutputArchive archive, String tag) throws IOException {}
        @Override
        public void deserialize(InputArchive archive, String tag) throws IOException {}
    }

    public static final int NOT = 0x10000000;
    public static final int CLIENTREQ = 0x10000001;
    public static final int CLIENTRESP = 0x10000002;
    public static final int CLIENTRO = 0x10000003;
    public static final int CLIENTRORESP = 0x10000004;

    public static class RecordSimConnMessage extends SimConnMessage {
        protected static final Logger LOG = LoggerFactory.getLogger(SimConnMessage.class);
        boolean isSend;
        long peerId;
        // not | qp
        int type = -1;
        Record record;

        public int getSimConnMessageType() {
            return simConnMessageType;
        }

        public RecordSimConnMessage(int simConnMessageType) {
            super(simConnMessageType);
            timestamp = System.nanoTime();
        }

        static public RecordSimConnMessage createForClientReq(long peerId, TxnHeader hdr, boolean isReadOnly) {
            RecordSimConnMessage m = new RecordSimConnMessage(HDR_TYPE);
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
            RecordSimConnMessage m = new RecordSimConnMessage(HDR_TYPE);
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
            super(QP_TYPE);
            if (!enabled) return;
            assert (qp != null);
            timestamp = System.nanoTime();
            this.isSend = isSend;
            this.peerId = peerId;
            this.type = qp.getType();
            this.record = qp;
        }

        public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(this,tag);
            a_.writeLong(timestamp, "");
            a_.writeBool(isSend, "");
            a_.writeLong(peerId, "");
            a_.writeInt(type, "");
            a_.writeRecord(record, "");
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(tag);
            timestamp = a_.readLong("");
            isSend = a_.readBool("");
            peerId = a_.readLong("");
            type = a_.readInt("");
            if (getSimConnMessageType() == QP_TYPE) {
                record = new QuorumPacket();
            } else {
                assert (getSimConnMessageType() == HDR_TYPE);
                record = new TxnHeader();
            }
            a_.readRecord(record, "");
            a_.endRecord(tag);
        }

        public String toString() {
            return String.format("ts=%d, %s, peer=%d, type=%d, record=%s", timestamp, isSend, peerId, type, record);
        }
    }
    // End of RecordSimConnMessage

    public static class BytesSimConnMessage extends SimConnMessage {
        protected static final Logger LOG = LoggerFactory.getLogger(SimConnMessage.class);
        long timestamp;
        boolean isSend;
        long peerId;
        // not | qp
        int type = -1;
        byte[] bytes;

        public BytesSimConnMessage() {
            super(BYTES_TYPE);
            if (!enabled) return;
            timestamp = System.nanoTime();
        }

        public BytesSimConnMessage(boolean isSend, long peerId, int type, byte[] bytes) {
            super(BYTES_TYPE);
            if (!enabled) return;
            this.timestamp = System.nanoTime();
            this.isSend = isSend;
            this.peerId = peerId;
            this.type = type;
            this.bytes = bytes;
        }

        public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
            LOG.info("BytesSimConnMessage bytes({}): {}", bytes.length, bytes);
            a_.startRecord(this,tag);
            a_.writeLong(timestamp, "");
            a_.writeBool(isSend, "");
            a_.writeLong(peerId, "");
            a_.writeInt(type, "");
            a_.writeBuffer(bytes, "");
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(tag);
            timestamp = a_.readLong("");
            isSend = a_.readBool("");
            peerId = a_.readLong("");
            type = a_.readInt("");
            bytes = a_.readBuffer("");
            a_.endRecord(tag);
        }
    }
    // End of BytesSimConnMessage


    protected static final Logger LOG = LoggerFactory.getLogger(SimConn.class);

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

    public static class AsyncSender extends Thread {
        // Note that BlockingQueue is thread-safe, we don't need additional lock.
        private final LinkedBlockingQueue<SimConnMessage> sendQueue;
        private OutputStream outConn;
        private OutputArchive outArchive;
        public long msgCount;
        final static public int NOT_READY = 0;
        final static public int RUNNING = 1;
        final static public int STOPPING = 2;
        final static public int SHUTDOWN = 3;
        public int state = NOT_READY;

        AsyncSender() {
            this.sendQueue = new LinkedBlockingQueue<>();
        }
        /*
         * Note that the `outConn` could even be a file stream
         */
        AsyncSender(OutputStream outConn) {
            this();
            this.outConn = outConn;
            updateOutArchive(outConn);
        }

        private void updateOutArchive(OutputStream outConn) {
            this.outConn =  outConn;
            this.outArchive = BinaryOutputArchive.getArchive(outConn);
        }

        public void run() {
            state = RUNNING;
            while (true) {
                try {
                    SimConnMessage m = sendQueue.take();
                    outArchive.writeInt(m.simConnMessageType, "");
                    m.serialize(outArchive, "packet");
                } catch (java.io.IOException | InterruptedException e) {
                    state = SHUTDOWN;
                    LOG.info(e.toString());
                    System.exit(1);
                }
            }
        }

        public void put(SimConnMessage m) {
            try {
                sendQueue.put(m);
            } catch (InterruptedException e) {
                LOG.info(e.toString());
                System.exit(1);
            }
        }

        protected void flush() throws IOException {
            outConn.flush();
        }
    }
    // End of AsyncSender

    public static class AsyncFileWriter extends AsyncSender {
        public static long intervalMilli = 30_000L;
        public static int queueSize = 262144;
        public long iid;
        public String tag;
        public String logDir;

        long startMsgCount = 0;

        public long queueBeginTs;
        public long fileBeginTs;

        public static class SendQueue {
            public ArrayList<SimConnMessage> queue;
            final public long beginTs;
            final public Path dataPath;
            final public Path metaPath;
            public SendQueue(long beginTs, int size, AsyncFileWriter afw) {
                this.beginTs = beginTs;
                queue = new ArrayList<>(size);
                final String formatTime = dateFormatter.format(java.time.Instant.ofEpochMilli(beginTs));
                Path timeSlotDir = Paths.get(afw.logDir, formatTime);
                timeSlotDir.toFile().mkdirs();
                dataPath = Paths.get(timeSlotDir.toString(), String.format("messages.%s.%s.data", afw.iid, afw.tag));
                metaPath = Paths.get(timeSlotDir.toString(), String.format("messages.%s.%s.meta", afw.iid, afw.tag));
            }
        }
        private SendQueue sendQueue;
        public ArrayBlockingQueue<SendQueue> immutableSendQueues;

        AsyncFileWriter(long iid, String tag) {
            super();
            this.iid = iid;
            this.tag = tag;
            this.logDir = "/tmp/log/log" + dateFormatter.format(java.time.Instant.ofEpochMilli(initTimeMilli)) + "/";
            if (!Files.exists(Paths.get(logDir)) && !new File(logDir).mkdirs()) {
                LOG.error("Failed to create dirs: {}", logDir);
                System.exit(-1);
            }
            // Every put message will have timestamp larger than current,
            // and so larger than fileBeginTs
            queueBeginTs = fileBeginTs = getCurrentBeginTs();
            sendQueue = new SendQueue(fileBeginTs, queueSize, this);
            immutableSendQueues = new ArrayBlockingQueue<>(4);
            state = NOT_READY;
        }

        public long getCurrentBeginTs() {
            long current = System.currentTimeMillis();
            return intervalMilli * ((current - initTimeMilli) / intervalMilli) + initTimeMilli;
        }

        @Override
        public synchronized void put(SimConnMessage m) {
            if (queueBeginTs + intervalMilli - initTimeMilli < (m.timestamp - initTimeNano)/1_000_000L
                    || m.simConnMessageType == SimConnMessage.TIMEOUT_TYPE) {
                queueBeginTs = queueBeginTs + intervalMilli;
                // Set the `prevSendQueue`
                int lastSize = sendQueue.queue.size();
                try {
                    immutableSendQueues.put(sendQueue);
                } catch (InterruptedException e) {
                    LOG.error("Shouldn't be interrupted");
                    System.exit(-1);
                }
                sendQueue = new SendQueue(queueBeginTs, lastSize, this);
            }
            if (m.simConnMessageType != SimConnMessage.TIMEOUT_TYPE) {
                sendQueue.queue.add(m);
            }
        }

        public void writeSendQueue(SendQueue sendQueue) throws IOException {
            OutputStream outConn = new BufferedOutputStream(Files.newOutputStream(sendQueue.dataPath), 8192);
            // Update outArchive
            OutputArchive outArchive = BinaryOutputArchive.getArchive(outConn);
            // Start writing
            int size = sendQueue.queue.size();
            long begin = System.currentTimeMillis();
            sendQueue.queue.stream().forEach(m -> {
                try {
                    msgCount++;
                    outArchive.writeInt(m.simConnMessageType, "");
                    m.serialize(outArchive, "packet");
                } catch (IOException e) {
                    LOG.error(e.toString());
                    System.exit(1);
                }
            });
            outConn.flush();
            outConn.close();
            // Write meta
            FileWriter metaWriter = new FileWriter(sendQueue.metaPath.toFile());
            metaWriter.write(String.format("%d\n%d\n", startMsgCount, msgCount));
            startMsgCount = msgCount;
            metaWriter.close();

            LOG.info("\033[1;32mThread {} done writing {}, takes {} ms, path={}\033[0m", tag, size, (System.currentTimeMillis()-begin), sendQueue.dataPath);
        }

        @Override
        public void run() {
            state = RUNNING;
            try {
                while (state == RUNNING) {
                    SendQueue sendQueue = null;
                    while (sendQueue == null && state == RUNNING) {
                        sendQueue = immutableSendQueues.poll(2, TimeUnit.SECONDS);
                        // This guarantees that if RUNNING, this thread can notify the master
                        // thread about the timeslot timeout if the master thread doesn't
                        // receive anything since timeslot timeout.
                        if (System.currentTimeMillis() - fileBeginTs > intervalMilli) {
                            put(new SimConnMessage(System.nanoTime(), SimConnMessage.TIMEOUT_TYPE));
                            fileBeginTs += intervalMilli;
                        }
                    }
                    if (sendQueue != null) {
                        writeSendQueue(sendQueue);
                    }
                }
                flush();
                state = SHUTDOWN;
            } catch (ConcurrentModificationException e) {
                LOG.error("\033[1;31m{} encountered ConcurrentModificationException\033[0m", tag);
                System.exit(-1);
            } catch (InterruptedException e) {
                LOG.info("SimConn ({}) exited due to InterruptedException", tag);
                System.exit(-1);
            } catch (IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }
        }

        @Override
        protected void flush() throws IOException {
            try {
                immutableSendQueues.put(sendQueue);
                sendQueue = null;
            } catch (InterruptedException e) {
                LOG.error("Flushing sendQueue to immutableSendQueues interrupted.");
                System.exit(-1);
            }
            while (!immutableSendQueues.isEmpty()) {
                try {
                    SendQueue sendQueue = immutableSendQueues.take();
                    writeSendQueue(sendQueue);
                } catch (InterruptedException e) {
                    LOG.error("Flushing immutableSendQueues interrupted.");
                    System.exit(-1);
                }
            }
        }
    }
    // End of AsyncFileWriter

    MODE mode;
    CONNTYPE conntype;

    // Just in case we may need it.
    private long iid;
    public long myid;
    public String tag;

    // NOTE: Don't directly use inConn/outConn, but use inArchive/outArchive
    private InputStream inConn;
    private OutputStream outConn;

    public AsyncSender asyncSender;

    public SimConn() {}

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

    public void init(final long myId, final String tag) {
        this.setupEnv();
        this.myid = myId;
        this.tag = tag;
        LOG.info("SimConn({}) setup: mode={}, conntype={}, iid={}, myid={}", tag, mode, conntype, iid, myid);

        if (mode == MODE.NONE) {
            return;
        }

        if (mode != MODE.FILE) {
            if (conntype == CONNTYPE.PIPE) {
                String stocPath = "/tmp/tmpPipe_s_to_c." + iid + ".pipe";
                String ctosPath = "/tmp/tmpPipe_c_to_s." + iid + ".pipe";
                try {
                    outConn = Files.newOutputStream(Paths.get(ctosPath));
                    inConn = Files.newInputStream(Paths.get(stocPath));
                } catch (Exception e) {
                    LOG.error(e.toString());
                    System.exit(1);
                }
                LOG.info("\033[1;31mSimConn Initialized. Connected to {} and {}\033[0m", stocPath, ctosPath);
            } else {
                LOG.error("Only PIPE conntype is implemented");
            }
        }

        // TODO: implement for SYNC
        switch (this.mode) {
            case ASYNC:
                asyncSender = new AsyncSender(outConn);
                asyncSender.start();
                break;
            case FILE:
                asyncSender = new AsyncFileWriter(iid, tag);
                asyncSender.start();
                break;
        }
    }

    public void request(SimConnMessage m) {
        if (mode != MODE.NONE) {
            if (mode == MODE.ASYNC || mode == MODE.FILE) {
                asyncSender.put(m);
            }
        }
    }

    public void flush() {
        try {
            asyncSender.flush();
        } catch (IOException e) {
            LOG.error(e.toString());
            System.exit(-1);
        }
    }

    public void stop() {
        if (enabled()) {
            asyncSender.put(new SimConnMessage(SimConnMessage.TIMEOUT_TYPE));
            asyncSender.state = AsyncSender.STOPPING;
        }
    }

    public void shutdown() {
        if (enabled()) {
            if (asyncSender.state != AsyncSender.STOPPING) {
                LOG.error("Should stop before shutdown.");
                System.exit(-1);
            }
            try {
                while (asyncSender.state != AsyncSender.SHUTDOWN) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                LOG.error("Waiting for writing done is interrupted.");
                System.exit(-1);
            }
        }
    }

    public boolean enabled() {
        return this.mode != MODE.NONE;
    }
}
