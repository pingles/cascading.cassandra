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

import java.util.HashMap;
import java.util.Map;

public class CassandraSchemeTest extends TestCase {
    private EmbeddedCassandraService service;
    private String inputFile = "./src/test/data/small.txt";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.service = new EmbeddedCassandraService();
        this.service.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        this.service.stop();
    }

    transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

    @Test
    public void testSomething() {
        // create flow to read from local file and insert into HBase
        Tap source = new Lfs(new TextLine(), inputFile);
        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(new Fields("num", "lower", "upper"), " "));
        Fields keyFields = new Fields( "num" );
        Fields[] valueFields = new Fields[]{new Fields("lower"), new Fields("upper")};

        Tap sink = new Lfs(new TextLine(), "./src/test/data/tmp-out.txt");
        Flow parseFlow = new FlowConnector(properties).connect(source, sink, parsePipe);
        parseFlow.complete();

        assert(true);
    }
}
