package org.pingles.cascading.cassandra;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import junit.framework.TestCase;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.KsDef;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class CassandraSchemeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSchemeTest.class);

    private static final String CASSANDRA_YAML = "./src/test/resources/cassandra.yaml";
    private String inputFile = "./src/test/data/small.txt";
    private final String keyspaceName = "TestKeyspace";
    private final String columnFamilyName = "TestColumnFamily";
    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void startCassandra() {
        cassandra = new EmbeddedCassandraService();
        try {
            cassandra.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void ensureTestKeyspace() throws Exception {
        CassandraClient client = new CassandraClient(getRpcHost(), getRpcPort());
        client.open();
        StringBuilder sb = new StringBuilder();
        for (KsDef ks : client.describeKeyspaces()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(ks.name);
        }
        LOGGER.info("Current keyspaces: {}", sb.toString());
        if (!client.keyspaceExists(keyspaceName)) {
            LOGGER.info("Creating test keyspace {}", keyspaceName);
            client.createKeyspace(keyspaceName);
        }
        client.close();
    }

    transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

    @Test
    public void testSomething() {
        // create flow to read from local file and insert into HBase
        Tap source = new Lfs(new TextLine(), inputFile);
        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(new Fields("num", "lower", "upper"), " "));
        Fields keyFields = new Fields("num");
        Fields valueFields = new Fields("lower", "upper");

        Tap sink = new CassandraTap(getRpcHost(), getRpcPort(), keyspaceName, columnFamilyName, new CassandraScheme(keyFields, valueFields));

        Flow parseFlow = new FlowConnector(properties).connect(source, sink, parsePipe);
        parseFlow.complete();

        assert(true);
    }

    protected Integer getRpcPort() {
        return (Integer) getCassandraConfiguration("rpc_port");
    }

    protected String getRpcHost() {
        return "127.0.0.1";
    }

    private Object getCassandraConfiguration(String fieldName) {
        Yaml y = new Yaml();
        try {
            Map m = (Map) y.load(new FileReader(CASSANDRA_YAML));
            return m.get(fieldName);
        } catch (FileNotFoundException e) {
            return null;
        }
    }
}
