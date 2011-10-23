package org.pingles.cascading.cassandra;

import org.apache.cassandra.thrift.CassandraDaemon;

import java.io.IOException;

public class EmbeddedCassandraService {
    private CassandraDaemon daemon;

    public EmbeddedCassandraService() throws IOException {
        daemon = new CassandraDaemon();
        daemon.init(null);
    }

    public void start() {
        daemon.start();
    }

    public void stop() {
        daemon.stop();
    }
}
