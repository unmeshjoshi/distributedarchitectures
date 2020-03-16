package org.dist.patterns.leaderelection;

import org.junit.Test;

public class ServerTest {

    @Test
    public void startup() {
        Server server = new Server();
        server.startup();
    }
}