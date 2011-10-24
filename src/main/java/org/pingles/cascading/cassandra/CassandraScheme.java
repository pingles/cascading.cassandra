package org.pingles.cascading.cassandra;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CassandraScheme extends Scheme {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheme.class);

    /**
     * Creates a {@link Scheme} for dealing with a regular Column Family.
     * @param keyFields     the fields to use for the row key
     * @param nameFields    column names
     */
    public CassandraScheme(Fields keyFields, Fields nameFields) {
    }

    @Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
    }

    @Override
    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
        jobConf.setOutputFormat(ColumnFamilyOutputFormat.class);
    }

    @Override
    public Tuple source(Object o, Object o1) {
        throw new NotImplementedException();
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
    }
}
