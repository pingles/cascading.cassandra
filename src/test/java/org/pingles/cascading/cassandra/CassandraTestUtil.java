package org.pingles.cascading.cassandra;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class CassandraTestUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraFlowTest.class);
    private static final String CASSANDRA_YAML = "./src/test/resources/cassandra.yaml";

    public static void ensureKeyspace(String name) throws TException, InterruptedException, InvalidRequestException, SchemaDisagreementException {
        CassandraClient client = new CassandraClient(getRpcHost(), getRpcPort());
        client.open();
        if (!client.keyspaceExists(name)) {
            LOGGER.info("Creating test keyspace {}", name);
            client.createKeyspace(name);
        }
        client.close();
    }

    public static void ensureColumnFamily(String keyspace, String columnFamily) throws TException, InterruptedException, NotFoundException, InvalidRequestException, SchemaDisagreementException {
        CassandraClient client = new CassandraClient(getRpcHost(), getRpcPort());
        client.open();
        if (!client.columnFamilyExists(keyspace, columnFamily)) {
            LOGGER.info("Creating test column family {}", columnFamily);
            client.createColumnFamily(keyspace, columnFamily);
        }
        client.close();
    }

    public static Integer getRpcPort() {
        return (Integer) getCassandraConfiguration("rpc_port");
    }

    public static String getRpcHost() {
        return "127.0.0.1";
    }

    public static Object getCassandraConfiguration(String fieldName) {
        Yaml y = new Yaml();
        try {
            Map m = (Map) y.load(new FileReader(CASSANDRA_YAML));
            return m.get(fieldName);
        } catch (FileNotFoundException e) {
            return null;
        }
    }
}
