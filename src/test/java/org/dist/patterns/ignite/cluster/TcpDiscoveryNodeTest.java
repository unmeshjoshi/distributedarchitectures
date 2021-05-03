package org.dist.patterns.ignite.cluster;

import org.junit.Test;

import static org.junit.Assert.*;

public class TcpDiscoveryNodeTest {

    @Test
    public void singleServerShouldMarkItSelfAsCoordinator() {
        new TcpDiscoveryNode();
    }

}