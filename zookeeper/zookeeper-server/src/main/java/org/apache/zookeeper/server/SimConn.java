package org.apache.zookeeper.server;

import org.apache.jute.*;
import org.apache.jute.Record;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class SimConn {
    public static boolean enabled = false;
    final static public DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneId.systemDefault());
    static final public long initTimeMilli = System.currentTimeMillis();

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

        @Override
        public void serialize(OutputArchive archive, String tag) throws IOException {}
        @Override
        public void deserialize(InputArchive archive, String tag) throws IOException {}
    }

    public static final int NOT = 0x1000_0000;
    public static final int CLIENTREQ = 0x1000_0001;
    public static final int CLIENTRESP = 0x1000_0002;
    public static final int CLIENTRO = 0x1000_0003;
    public static final int CLIENTRORESP = 0x1000_0004;
    public static final int BIGSYNC = 0x1000_0005;
    public static final int TCP = 0x1000_0006;

    public static class RecordSimConnMessage extends SimConnMessage {
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

    public static class RecordArraySimConnMessage extends RecordSimConnMessage {
        // This is only for the sync
        public ArrayList<QuorumPacket> qps;
        public RecordArraySimConnMessage(long peerId, boolean isSend, ArrayList<QuorumPacket> qps) {
            super(QP_TYPE);
            if (!enabled) return;
            assert (qps != null);
            timestamp = System.nanoTime();
            this.isSend = isSend;
            this.peerId = peerId;
            this.type = BIGSYNC;
            this.qps = qps;
        }

        public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(this,tag);
            a_.writeLong(timestamp, "");
            a_.writeBool(isSend, "");
            a_.writeLong(peerId, "");
            a_.writeInt(type, "");
            a_.writeInt(this.qps.size(), "");
            for (QuorumPacket qp : this.qps) {
                a_.writeRecord(qp, "");
            }
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(tag);
            timestamp = a_.readLong("");
            isSend = a_.readBool("");
            peerId = a_.readLong("");
            type = a_.readInt("");
            int size = a_.readInt("");
            qps = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                QuorumPacket qp = new QuorumPacket();
                a_.readRecord(qp, "");
                qps.add(qp);
            }
            a_.endRecord(tag);
        }
    }
    // End of RecordArraySimConnMessage

    public static class BytesSimConnMessage extends SimConnMessage {
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

        public static BytesSimConnMessage createForTCP(long peerId) {
            BytesSimConnMessage m = new BytesSimConnMessage();
            m.isSend = false;
            m.peerId = peerId;
            m.type = TCP;
            m.bytes = new byte[0];
            return m;
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

    public enum MODE {
        NONE,
        ASYNC,
        SYNC,
        FILE
    }

    public enum CONNTYPE {
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
        public volatile int state = NOT_READY;

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
            ByteArrayOutputStream tmpOutputStream = new ByteArrayOutputStream(2048);
            while (true) {
                try {
                    SimConnMessage m = sendQueue.take();
                    tmpOutputStream.reset();
                    DataOutputStream tmpDos = new DataOutputStream(tmpOutputStream);
                    OutputArchive tmpOutArchive = BinaryOutputArchive.getArchive(tmpDos);
                    m.serialize(tmpOutArchive, "");
                    long length = 4 + tmpDos.size();
                    outArchive.writeLong(length, "length");
                    outArchive.writeInt(m.simConnMessageType, "");
                    m.serialize(outArchive, "packet");
                } catch (java.io.IOException | InterruptedException e) {
                    state = SHUTDOWN;
                    LOG.error(e.toString());
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
        public static long intervalMilli = 1_000L;
        public static int queueSize = 262144;
        public long iid;
        public String tag;
        public String logDir;

        long startMsgCount = 0;

        public static class SendQueue {
            public ArrayList<SimConnMessage> queue;
            public long beginTs;
            public Path dataPath;
            public Path metaPath;
            final public AsyncFileWriter afw;
            public SendQueue(long beginTs, int size, AsyncFileWriter afw) {
                this.beginTs = beginTs;
                this.afw = afw;
                queue = new ArrayList<>(size);
                if (beginTs != 0) {
                    ResetLogPath(beginTs);
                }
            }

            public void ResetLogPath(long tsMilli) {
                final String formatTime = dateFormatter.format(java.time.Instant.ofEpochMilli(tsMilli));
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
            // The first message sets the time slot (see put()).
            sendQueue = new SendQueue(0, queueSize, this);
            immutableSendQueues = new ArrayBlockingQueue<>(4);
            state = NOT_READY;
        }

        public long getPeriodTs(long tsMilli) {
            return intervalMilli * (tsMilli / intervalMilli);
        }

        @Override
        public synchronized void put(SimConnMessage m) {
            if (sendQueue == null) {
                // Already flushed (shut down); drop late messages.
                return;
            }
            if (m.simConnMessageType == SimConnMessage.TIMEOUT_TYPE) {
                // Stop: hand over the current time slot, if it has messages.
                if (sendQueue.dataPath != null) {
                    rotate(0);
                }
                return;
            }
            long mTsMilli = m.timestamp/1_000_000L;
            if (sendQueue.beginTs == 0) {
                sendQueue.beginTs = getPeriodTs(mTsMilli);
                sendQueue.ResetLogPath(sendQueue.beginTs);
            }
            long mTimeslotBegin = getPeriodTs(mTsMilli);
            if (mTimeslotBegin > sendQueue.beginTs) {
                rotate(mTimeslotBegin);
            }
            sendQueue.queue.add(m);
        }

        /* Hands the current time slot to the writer thread and starts a new
         * one beginning at newBeginTs (0: set by the next message). */
        private void rotate(long newBeginTs) {
            int lastSize = sendQueue.queue.size();
            try {
                immutableSendQueues.put(sendQueue);
            } catch (InterruptedException e) {
                LOG.error("Shouldn't be interrupted");
                System.exit(-1);
            }
            sendQueue = new SendQueue(newBeginTs, lastSize, this);
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

            LOG.debug("SimConn({}) wrote {} messages in {} ms to {}", tag, size, (System.currentTimeMillis() - begin), sendQueue.dataPath);
        }

        @Override
        public void run() {
            try {
                while (state == RUNNING) {
                    SendQueue sendQueue = immutableSendQueues.poll(2, TimeUnit.SECONDS);
                    if (sendQueue != null) {
                        writeSendQueue(sendQueue);
                    }
                }
                flush();
                state = SHUTDOWN;
            } catch (ConcurrentModificationException e) {
                LOG.error("SimConn({}) encountered ConcurrentModificationException", tag);
                System.exit(-1);
            } catch (InterruptedException e) {
                LOG.info("SimConn ({}) exited due to InterruptedException", tag);
                System.exit(-1);
            } catch (IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }
        }

        /* Writes all pending time slots, including the current one. Called by
         * the writer thread when it stops. */
        @Override
        protected void flush() throws IOException {
            SendQueue last;
            synchronized (this) {
                last = sendQueue;
                sendQueue = null;
            }
            SendQueue pending;
            while ((pending = immutableSendQueues.poll()) != null) {
                writeSendQueue(pending);
            }
            if (last != null && last.dataPath != null) {
                writeSendQueue(last);
            }
        }
    }
    // End of AsyncFileWriter

    public MODE mode = MODE.NONE;
    public CONNTYPE conntype;

    // Just in case we may need it.
    private long iid;
    public long myid;
    public String tag;

    // inConn is opened so that the simulator can open the write end of its
    // pipe; nothing is read from it.
    private InputStream inConn;
    private OutputStream outConn;

    public AsyncSender asyncSender;

    public SimConn() {}

    public void setupEnv() {
        final String simmodeStr = System.getenv("SIMMODE");
        if (simmodeStr == null || simmodeStr.equals("NONE")) {
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
            System.exit(1);
        }

        if (this.mode == MODE.SYNC) {
            LOG.error("SIMMODE=SYNC is not implemented for ZooKeeper; use ASYNC or FILE");
            System.exit(1);
        }
        if (this.mode != MODE.NONE && this.conntype != CONNTYPE.PIPE && this.mode != MODE.FILE) {
            LOG.error("Only SIMCONNTYPE=PIPE is implemented for ZooKeeper");
            System.exit(1);
        }

        if (this.mode != MODE.NONE) {
            final String iidStr = System.getenv("IID");
            try {
                this.iid = Long.parseLong(iidStr);
            } catch (NumberFormatException e) {
                LOG.error("IID must be set to a number when SIMMODE is " + simmodeStr + " (got: " + iidStr + ")");
                System.exit(1);
            }
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
                LOG.info("SimConn initialized. Connected to {} and {}", stocPath, ctosPath);
            }
        }

        switch (this.mode) {
            case ASYNC:
                asyncSender = new AsyncSender(outConn);
                break;
            case FILE:
                asyncSender = new AsyncFileWriter(iid, tag);
                break;
            default:
                return;
        }
        asyncSender.state = AsyncSender.RUNNING;
        asyncSender.start();
    }

    public void request(SimConnMessage m) {
        if (mode != MODE.NONE) {
            if (mode == MODE.ASYNC || mode == MODE.FILE) {
                asyncSender.put(m);
            }
        }
    }

    /* Stops reporting. In FILE mode, the current time slot is handed to the
     * writer thread, which writes it and exits. In ASYNC mode the sender
     * thread keeps draining its queue onto the pipe. */
    public void stop() {
        if (enabled() && mode == MODE.FILE && asyncSender.state == AsyncSender.RUNNING) {
            asyncSender.put(new SimConnMessage(SimConnMessage.TIMEOUT_TYPE));
            asyncSender.state = AsyncSender.STOPPING;
        }
    }

    /* Stops reporting and, in FILE mode, waits until all log files are written. */
    public void shutdown() {
        stop();
        if (enabled() && mode == MODE.FILE) {
            try {
                while (asyncSender.state != AsyncSender.SHUTDOWN) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean enabled() {
        return this.mode != MODE.NONE;
    }
}
