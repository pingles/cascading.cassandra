package org.pingles.cascading.cassandra;

import org.apache.cassandra.service.CassandraDaemon;

import java.io.IOException;

public class EmbeddedCassandraService {
    private final CassandraDaemon daemon;

    public EmbeddedCassandraService() throws IOException {
        daemon = new org.apache.cassandra.thrift.CassandraDaemon();
        daemon.init(null);
    }

    public void start() throws IOException {
        daemon.start();
    }

    public void stop() {
        daemon.stop();
    }
}
