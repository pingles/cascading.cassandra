package org.pingles.cascading.cassandra;

import junit.framework.TestCase;
import org.junit.Test;

public class CassandraSchemeTest extends TestCase {
    private EmbeddedCassandraService service;

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

    @Test
    public void testSomething() {
        assert(true);
    }
}
