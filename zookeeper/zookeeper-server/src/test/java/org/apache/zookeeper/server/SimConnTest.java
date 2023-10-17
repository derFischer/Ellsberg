package org.apache.zookeeper.server;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

public class SimConnTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(SimConnTest.class);
    @Test
    public void parseLog() {
        ArrayList<SimConn.SimConnMessage> msgs = new ArrayList<>();
        try {
            DataInputStream dis = new DataInputStream(new FileInputStream("/mydata/zk.tslog/messages.5.20231015-014129.data"));
            BinaryInputArchive bia = new BinaryInputArchive(dis);
            while (true) {
                try {
                    int simConnMessageType = bia.readInt("");
                    if (simConnMessageType == SimConn.SimConnMessage.BYTES_TYPE) {
                        SimConn.BytesSimConnMessage m = new SimConn.BytesSimConnMessage();
                        m.deserialize(bia, "");
                        msgs.add(m);
                        if (msgs.size() % 100000 == 0)
                            LOG.info("{} B {}", msgs.size(), m);
                    } else {
                        SimConn.RecordSimConnMessage m = new SimConn.RecordSimConnMessage(simConnMessageType);
                        m.simConnMessageType = simConnMessageType;
                        m.deserialize(bia, "");
                        msgs.add(m);
                        if (msgs.size() % 100000 == 0)
                            LOG.info("{} R {}", msgs.size(), m);
                    }
                } catch (EOFException e) {
                    LOG.info("Messages prepared, got {} messages.", msgs.size());
                    break;
                }
            }
        } catch(FileNotFoundException e) {
            LOG.error("File not found");
            System.exit(-1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        SimConn simConn = new SimConn();
        simConn.setupEnv();
        simConn.init(1, "");
        LOG.info("Start writing");
        msgs.forEach(simConn::request);
        LOG.info("End writing");
    }

    @Test
    public void testLinkedList() {
        LinkedList<SimConn.SimConnMessage> queue = new LinkedList<>();
        for (int i = 0; i < 100000; i++) {
            long begin = System.currentTimeMillis();
            for (int j = 0; j < 100000; j++) {
                queue.add(new SimConn.RecordSimConnMessage(5, false, new QuorumPacket(Leader.COMMIT, 0, new byte[300], null)));
            }
            long end = System.currentTimeMillis();
            LOG.info("iter {}: {} ms", i, end-begin);
        }
    }

}
