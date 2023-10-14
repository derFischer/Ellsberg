package org.apache.zookeeper.server;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.junit.jupiter.api.Test;

public class SimConnTest extends ZKTestCase {
    @Test
    public void benchmark() {
        SimConn simConn = new SimConn();
        simConn.setupEnv();
        simConn.init(1);
        QuorumPacket commit = new QuorumPacket(Leader.COMMIT, 0x0000000100000001L, null, null);
    }

}
