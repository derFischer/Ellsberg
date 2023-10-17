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
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimConn {
    public static boolean enabled = false;
    final static public DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    final public static String initTime = dateFormatter.format(ZonedDateTime.now());

    public static class SimConnMessage implements Record {
        final public static int TIMEOUT_TYPE = 0;
        final public static int BYTES_TYPE = 1;
        final public static int QP_TYPE = 2;
        final public static int HDR_TYPE = 3;
        final int simConnMessageType;
        long timestamp;

        public SimConnMessage() {
            simConnMessageType = -1;
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
        int simConnMessageType = -1;
        boolean isSend;
        long peerId;
        // not | qp
        int type = -1;
        Record record;

        public int getSimConnMessageType() {
            return simConnMessageType;
        }

        public RecordSimConnMessage(int simConnMessageType) {
            this.simConnMessageType = simConnMessageType;
            timestamp = System.currentTimeMillis();
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
            if (!enabled) return;
            assert (qp != null);
            simConnMessageType = QP_TYPE;
            timestamp = System.currentTimeMillis();
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
        final static byte[] emptyByteArray = new byte[]{};
        boolean isSend;
        long peerId;
        // not | qp
        int recvType = -1;
        int sendType = -1;
        byte[] recv;
        byte[] send;

        long timestamp;

        public BytesSimConnMessage() {
            timestamp = System.currentTimeMillis();
        }

        public BytesSimConnMessage(long peerId, int type, byte[] recv, byte[] send) {
            if (!enabled) return;
            assert (recv != null || send != null);
            timestamp = System.currentTimeMillis();
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
            a_.writeLong(timestamp, "");
            a_.writeBool(isSend, "");
            a_.writeLong(peerId, "");
            a_.writeInt(recvType, "");
            a_.writeInt(sendType, "");
            a_.writeBuffer(recv, "");
            a_.writeBuffer(send, "");
            a_.endRecord(this, tag);
        }

        public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
            a_.startRecord(tag);
            timestamp = a_.readLong("");
            isSend = a_.readBool("");
            peerId = a_.readLong("");
            recvType = a_.readInt("");
            sendType = a_.readInt("");
            recv = a_.readBuffer("");
            send = a_.readBuffer("");
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
        private LinkedBlockingQueue<SimConnMessage> sendQueue;
        protected OutputStream outConn;
        protected OutputArchive outArchive;
        public long msgCount;

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

        public void updateOutArchive(OutputStream outConn) {
            this.outConn =  outConn;
            this.outArchive = BinaryOutputArchive.getArchive(outConn);
        }

        public void run() {
            while (true) {
                try {
                    SimConnMessage m = sendQueue.take();
                    outArchive.writeInt(m.simConnMessageType, "");
                    m.serialize(outArchive, "packet");
                } catch (java.io.IOException | InterruptedException e) {
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

        public void flush() throws IOException {
            outConn.flush();
        }
    }
    // End of AsyncSender

    public static class AsyncFileWriter extends AsyncSender {
        public static long warmupDelay = 10 * 1000L;
        public static long filePeriod = 10 * 1000L;
        public static int queueSize = 262144;
        public long iid;
        public String tag;
        public String logDir;
        public Path metaFilePath;

        public long beginTs;
        public long interval;

        public static class SendQueue {
            public ArrayList<SimConnMessage> queue;
            final public String date;
            public SendQueue(String date, int size) {
                this.date = date;
                queue = new ArrayList<>(size);
            }
        }
        private SendQueue sendQueue;
        public ArrayBlockingQueue<SendQueue> immutableSendQueues;

        AsyncFileWriter(long iid, String tag) {
            super();
            this.iid = iid;
            this.tag = tag;
            this.logDir = "/tmp/log/log" + initTime + "/" + tag;
            if (!Files.exists(Paths.get(logDir)) && !new File(logDir).mkdirs()) {
                LOG.error("Failed to create dirs: {}", logDir);
                System.exit(-1);
            }
            changeFile();
            beginTs = System.currentTimeMillis();
            interval = warmupDelay;
            sendQueue = new SendQueue(initTime, queueSize);
            immutableSendQueues = new ArrayBlockingQueue<>(4);
        }

        public void changeFile() {
            String formatTime = dateFormatter.format(ZonedDateTime.now());
            Path path = Paths.get(this.logDir, String.format("messages.%s.%s.data", iid, formatTime));
            try {
                outConn = new BufferedOutputStream(Files.newOutputStream(path), 8192);
            } catch (IOException e) {
                LOG.info(e.toString());
                System.exit(1);
            }
            metaFilePath = Paths.get(this.logDir, String.format("messages.%s.%s.meta", iid, formatTime));
            updateOutArchive(outConn);
            LOG.info("use next filepath: {}", path);
        }

        // There should be only one thread calling this.
        @Override
        public void put(SimConnMessage m) {
            long current = System.currentTimeMillis();
            if (beginTs + interval < current) {
                beginTs = current;
                interval = filePeriod;
                // Set the `prevSendQueue`
                int lastSize = sendQueue.queue.size();
                try {
                    immutableSendQueues.put(sendQueue);
                    LOG.info("\033[1;31m{} putted, now {}\033[0m", tag, immutableSendQueues.size());
                } catch (InterruptedException e) {
                    LOG.error("Shouldn't be interrupted");
                    System.exit(-1);
                }
                sendQueue = new SendQueue(dateFormatter.format(ZonedDateTime.now()), lastSize);
            }
            sendQueue.queue.add(m);
        }

        @Override
        public void run() {
            long startMsgCount = 0;
            try {
                Thread.sleep(warmupDelay);
                while (true) {
                    SendQueue sendQueue = immutableSendQueues.take();
                    int size = sendQueue.queue.size();
                    LOG.info("\033[1;32m{} start writing {}\033[0m", tag, size);
                    long begin = System.currentTimeMillis();
                    // while (!sendQueue.queue.isEmpty()) {
                    //     SimConnMessage m = sendQueue.queue.remove(0);
                    //     msgCount++;
                    //     outArchive.writeInt(m.simConnMessageType, "");
                    //     m.serialize(outArchive, "packet");
                    // }
                    sendQueue.queue.stream().forEach(m -> {
                        msgCount++;
                        try {
                            outArchive.writeInt(m.simConnMessageType, "");
                            m.serialize(outArchive, "packet");
                        } catch (IOException e) {
                            LOG.error(e.toString());
                            System.exit(1);
                        }
                    });
                    FileWriter metaWriter = new FileWriter(metaFilePath.toFile());
                    metaWriter.write(String.format("%d\n%d\n", startMsgCount, msgCount));
                    startMsgCount = msgCount;
                    metaWriter.close();
                    changeFile();
                    LOG.info("\033[1;32m{} done writing {}, takes {}\033[0m", tag, size, (System.currentTimeMillis()-begin)/1000);
                }
            } catch (ConcurrentModificationException e) {
                LOG.error("\033[1;31m{} encountered ConcurrentModificationException\033[0m", tag);
                System.exit(-1);
            } catch (InterruptedException e) {
                LOG.info("SimConn ({}) exited due to InterruptedException", tag);
            } catch (IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }
        }

        @Override
        public void flush() throws IOException {
            outConn.flush();
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

    public boolean enabled() {
        return this.mode != MODE.NONE;
    }
}
