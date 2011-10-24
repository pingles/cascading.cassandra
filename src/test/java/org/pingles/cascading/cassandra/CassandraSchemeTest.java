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
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class CassandraSchemeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSchemeTest.class);
    private static EmbeddedCassandraService cassandra;

    private String inputFile = "./src/test/data/small.txt";
    private final String keyspaceName = "TestKeyspace";
    private final String columnFamilyName = "TestColumnFamily";
    transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

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
    public void ensureCassandraTestStores() throws Exception {
        CassandraTestUtil.ensureKeyspace(keyspaceName);
        CassandraTestUtil.ensureColumnFamily(keyspaceName, columnFamilyName);
    }

    @Test
    public void testSomething() {
        // create flow to read from local file and insert into HBase
        Tap source = new Lfs(new TextLine(), inputFile);
        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(new Fields("num", "lower", "upper"), " "));
        Fields keyFields = new Fields("num");
        Fields valueFields = new Fields("lower", "upper");

        Tap sink = new CassandraTap(CassandraTestUtil.getRpcHost(), CassandraTestUtil.getRpcPort(), keyspaceName, columnFamilyName, new CassandraScheme(keyFields, valueFields));

        Flow parseFlow = new FlowConnector(properties).connect(source, sink, parsePipe);
        parseFlow.complete();

        assert(true);
    }

}
