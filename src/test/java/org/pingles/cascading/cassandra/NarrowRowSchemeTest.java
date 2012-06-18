package org.pingles.cascading.cassandra;

import cascading.tuple.Fields;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class NarrowRowSchemeTest {

    @Test
    public void shouldRaiseErrorWhenColumnNamesNotSameLengthAsFields() {
        try {
            NarrowRowScheme scheme = new NarrowRowScheme(new Fields("key"), new Fields("name", "age"), new String[] {"blah"});
            fail("Didn't throw exception");
        } catch (IllegalArgumentException e) {
            // success!
        }
    }
}
