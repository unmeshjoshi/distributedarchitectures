package org.dist.patterns.common;


import org.dist.kvstore.InetAddressAndPort;
import org.dist.patterns.replicatedlog.Client;
import org.dist.queue.TestUtils;
import org.dist.queue.api.RequestOrResponse;
import org.dist.util.Networks;
import org.junit.Test;

import java.net.InetAddress;

public class TcpListenerTest {

    @Test
    public void shouldExecuteSingularUpdateQueue() {
        InetAddress inetAddress = new Networks().ipv4Address();
        InetAddressAndPort serverIp = InetAddressAndPort.create(inetAddress.getHostAddress(), TestUtils.choosePort());
        TcpListener tcpListener = new TcpListener(serverIp);
        tcpListener.start();

        new Client().sendReceive(new RequestOrResponse((short) 1, "Test String", 0), serverIp);

    }
}