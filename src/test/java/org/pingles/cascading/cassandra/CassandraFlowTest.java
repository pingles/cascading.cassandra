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
import me.prettyprint.cassandra.serializers.TypeInferringSerializer;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class CassandraFlowTest {
    private static EmbeddedCassandraService cassandra;

    private final String keyspaceName = "TestKeyspace";
    private final String columnFamilyName = "TestColumnFamily";
    transient private static Map<Object, Object> properties = new HashMap<Object, Object>();
    private CassandraClient client;

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
    public void beforeTest() throws Exception {
        CassandraTestUtil.ensureKeyspace(keyspaceName);
        CassandraTestUtil.ensureColumnFamily(keyspaceName, columnFamilyName);
        client = new CassandraClient(getRpcHost(), getRpcPort(), keyspaceName);
        client.open();
    }

    @After
    public void afterTest() {
        client.close();
    }

    @Test
    public void testCassandraAsSink() throws Exception {
        String inputFile = "./src/test/data/small.txt";
        Tap source = new Lfs(new TextLine(), inputFile);
        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(new Fields("num", "lower", "upper"), " "));
        Fields keyFields = new Fields("num");
        Fields[] valueFields = new Fields[] {new Fields("lower"), new Fields("upper")};

        CassandraScheme scheme = new CassandraScheme(keyFields, valueFields);
        Tap sink = new CassandraTap(getRpcHost(), getRpcPort(), keyspaceName, columnFamilyName, scheme);

        Flow parseFlow = new FlowConnector(properties).connect(source, sink, parsePipe);
        parseFlow.complete();

        assertEquals("a", getTestBytes("1", "lower"));
        assertEquals("A", getTestBytes("1", "upper"));
        assertEquals("b", getTestBytes("2", "lower"));
        assertEquals("B", getTestBytes("2", "upper"));
    }

    private String getTestBytes(String key, String name) throws Exception {
        return bytesToString(client.getValue(columnFamilyName, toBytes(key), toBytes(name)));
    }

    private String bytesToString(byte[] bytes) {
        return new String(bytes);
    }

    private ByteBuffer toBytes(Object obj) {
        return TypeInferringSerializer.get().toByteBuffer(obj);
    }

    private String getRpcHost() {
        return CassandraTestUtil.getRpcHost();
    }

    private Integer getRpcPort() {
        return CassandraTestUtil.getRpcPort();
    }
}
