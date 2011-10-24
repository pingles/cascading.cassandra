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
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class CassandraSchemeTest extends CassandraTest {
    private String inputFile = "./src/test/data/small.txt";
    private final String keyspaceName = "TestKeyspace";
    private final String columnFamilyName = "TestColumnFamily";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        CassandraClient client = new CassandraClient(getRpcHost(), getRpcPort());
        client.open();
        if (!client.keyspaceExists(keyspaceName)) {
            client.createKeyspace(keyspaceName);
        }
        client.close();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
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
}
