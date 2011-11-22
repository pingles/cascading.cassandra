package org.pingles.cascading.cassandra;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuples;
import me.prettyprint.cassandra.serializers.TypeInferringSerializer;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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
        client.useKeyspace(keyspaceName);
    }

    @After
    public void afterTest() throws TException, InvalidRequestException, UnavailableException {
        try {
            client.truncate(columnFamilyName);
        } finally {
            client.close();
        }
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

    @Test
    public void testCassandraAsSource() throws Exception {
        client.put(columnFamilyName, toBytes("21"), toBytes("lower"), toBytes("a"));
        client.put(columnFamilyName, toBytes("21"), toBytes("upper"), toBytes("A"));
        client.put(columnFamilyName, toBytes("22"), toBytes("lower"), toBytes("b"));
        client.put(columnFamilyName, toBytes("22"), toBytes("upper"), toBytes("B"));

        CassandraScheme scheme = new CassandraScheme(new Fields("lower", "upper"));
        Tap source = new CassandraTap(getRpcHost(), getRpcPort(), keyspaceName, columnFamilyName, scheme);
        Tap sink = new Lfs(new TextLine(), "./tmp/test/cassandraAsSourceOutput.txt", SinkMode.REPLACE);
        Pipe copyPipe = new Each("read", new ByteBufferToString(new Fields("lower", "upper")));
        Flow copyFlow = new FlowConnector(properties).connect(source, sink, copyPipe);
        copyFlow.complete();

        List<String> outputContents = readLines("./tmp/test/cassandraAsSourceOutput.txt/part-00000");
        assertEquals(2, outputContents.size());
        assertEquals("a\tA", outputContents.get(0));
        assertEquals("b\tB", outputContents.get(1));
    }

    @Test
    public void testSourceSpecifyingColumnNames() throws Exception {
        client.put(columnFamilyName, toBytes("21"), toBytes("lower"), toBytes("a"));
        client.put(columnFamilyName, toBytes("21"), toBytes("upper"), toBytes("A"));
        client.put(columnFamilyName, toBytes("22"), toBytes("lower"), toBytes("b"));
        client.put(columnFamilyName, toBytes("22"), toBytes("upper"), toBytes("B"));

        Fields inputFields = new Fields("lower", "upper");
        Fields outputFields = new Fields("upper");
        CassandraScheme scheme = new CassandraScheme(inputFields);
        Tap source = new CassandraTap(getRpcHost(), getRpcPort(), keyspaceName, columnFamilyName, scheme);
        Tap sink = new Lfs(new TextLine(), "./tmp/test/cassandraAsSourceOutput.txt", SinkMode.REPLACE);

        Pipe copyPipe = new Each("read", new ByteBufferToString(inputFields), inputFields);
        copyPipe = new Each(copyPipe, outputFields, new Identity());
        Flow copyFlow = new FlowConnector(properties).connect(source, sink, copyPipe);
        copyFlow.complete();

        List<String> outputContents = readLines("./tmp/test/cassandraAsSourceOutput.txt/part-00000");
        assertEquals(2, outputContents.size());
        assertEquals("A", outputContents.get(0));
        assertEquals("B", outputContents.get(1));
    }

    private List<String> readLines(String fileName) throws IOException {
        FileInputStream inputStream = new FileInputStream(fileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        List<String> lines = new ArrayList<String>();
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        return lines;
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
