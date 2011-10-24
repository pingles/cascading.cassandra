package org.pingles.cascading.cassandra;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

public class CassandraClient {
    private TSocket socket;
    private TBinaryProtocol protocol;
    private Cassandra.Client client;

    public CassandraClient(String rpcHost, Integer rpcPort) {
        this.socket = new TSocket(rpcHost, rpcPort);
        this.protocol = new TBinaryProtocol(socket);
        this.client = new Cassandra.Client(protocol);
    }

    public String createKeyspace(String keyspaceName) throws TException, SchemaDisagreementException, InvalidRequestException {
        List<CfDef> columnFamilyDefs = new ArrayList<CfDef>();

        KsDef ksDef = new KsDef(keyspaceName, "org.apache.cassandra.locator.RackUnawareStrategy", columnFamilyDefs);

        this.client.send_system_add_keyspace(ksDef);
        return this.client.recv_system_add_keyspace();
    }

    public List<KsDef> describeKeyspaces() throws TException, InvalidRequestException {
        this.client.send_describe_keyspaces();
        return this.client.recv_describe_keyspaces();
    }

    public void open() throws TTransportException {
        this.socket.open();
    }

    public void close() {
        this.socket.close();
    }

    public boolean keyspaceExists(String keyspaceName) throws TException, InvalidRequestException {
        List<KsDef> keyspaces = describeKeyspaces();

        for (KsDef ksDef : keyspaces) {
            if (ksDef.name == keyspaceName) {
                return true;
            }
        }

        return false;
    }
}
