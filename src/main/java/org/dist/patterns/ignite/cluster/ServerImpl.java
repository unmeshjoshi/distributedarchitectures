package org.dist.patterns.ignite.cluster;


import org.dist.patterns.common.InetAddressAndPort;
import org.dist.patterns.common.RequestOrResponse;
import org.dist.patterns.common.TcpListener;

import static org.dist.patterns.ignite.cluster.TcpDiscoverySpiState.DISCONNECTED;

public class ServerImpl {
    private InetAddressAndPort listenAddress;
    private TcpListener tcpServ;

    public ServerImpl(InetAddressAndPort listenAddress) {
        this.listenAddress = listenAddress;
    }

    protected TcpDiscoverySpiState spiState = DISCONNECTED;

    public void spiStart(String gridName) {
        spiState = DISCONNECTED;
        tcpServ = new TcpListener(listenAddress);
    }

    public RequestOrResponse process(RequestOrResponse requestOrResponse) {
        return requestOrResponse;
    }
}
