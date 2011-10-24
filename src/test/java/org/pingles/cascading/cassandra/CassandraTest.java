package org.pingles.cascading.cassandra;

import junit.framework.TestCase;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class CassandraTest extends TestCase {
    private static final String CASSANDRA_YAML = "./src/test/resources/cassandra.yaml";
    private EmbeddedCassandraService service;

    protected Integer getRpcPort() {
        return (Integer) getCassandraConfiguration("rpc_port");
    }

    protected String getRpcHost() {
        return (String) getCassandraConfiguration("rpc_address");
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        startEmbeddedCassandra();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        stopEmbeddedCassandra();
    }

    private void stopEmbeddedCassandra() {
        this.service.stop();
    }

    public void startEmbeddedCassandra() throws IOException {
        this.service = new EmbeddedCassandraService();
        this.service.start();
    }
}
