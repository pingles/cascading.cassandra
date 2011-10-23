package org.pingles.cascading.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class CassandraClient {
    private TSocket socket;
    private TBinaryProtocol protocol;
    private Cassandra.Client client;

    public CassandraClient() {
        this.socket = new TSocket("localhost", 9100);
        this.protocol = new TBinaryProtocol(socket);
        this.client = new Cassandra.Client(protocol);
    }

    public void open() throws TTransportException {
        this.socket.open();
    }

    public void close() {
        this.socket.close();
    }
}
