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
import sun.rmi.runtime.Log;

import java.io.IOException;

public class CassandraScheme extends Scheme {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheme.class);

    public CassandraScheme(Fields keyFields, Fields valueFields) {
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
