package org.dist.patterns.common;


import org.dist.util.Networks;
import org.dist.utils.JTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;

import static org.junit.Assert.assertNotNull;

public class TcpListenerTest {

    @Test
    public void shouldExecuteSingularUpdateQueue() throws IOException {
        InetAddress inetAddress = new Networks().ipv4Address();
        InetAddressAndPort serverIp = InetAddressAndPort.create(inetAddress.getHostAddress(), JTestUtils.choosePort());
        TcpListener tcpListener = new TcpListener(serverIp);
        tcpListener.start();

        RequestOrResponse request = new RequestOrResponse(1, "Test String", 0);
        RequestOrResponse response = new Client().sendReceive(request, serverIp);

        assertNotNull(response);
    }
}