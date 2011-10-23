package org.pingles.cascading.cassandra;

import org.apache.cassandra.thrift.CassandraDaemon;

public class EmbeddedCassandraService {
    private CassandraDaemon daemon;

    public EmbeddedCassandraService() {
        daemon = new CassandraDaemon();
    }

    public void start() {
        daemon.start();
    }

    public void stop() {
        daemon.stop();
    }
}
