package org.pingles.cascading.cassandra;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import me.prettyprint.cassandra.serializers.TypeInferringSerializer;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class CassandraScheme extends Scheme {
    private final Fields keyField;
    private final Fields[] columnFields;

    /**
     * Creates a {@link Scheme} for dealing with a regular Column Family.
     * @param keyField      the field to use for the row key
     * @param columnFields  column names
     */
    public CassandraScheme(Fields keyField, Fields[] columnFields) {
        this.keyField = keyField;
        this.columnFields = columnFields;
    }

    @Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
    }

    @Override
    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
        jobConf.setOutputKeyClass(ByteBuffer.class);
        jobConf.setOutputValueClass(Mutation.class);
        jobConf.setOutputFormat(ColumnFamilyOutputFormat.class);
    }

    @Override
    public Tuple source(Object o, Object o1) {
        throw new NotImplementedException();
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        Tuple key = tupleEntry.selectTuple(keyField);
        TypeInferringSerializer<Object> serializer = TypeInferringSerializer.get();
        byte[] keyBytes = serializer.toBytes(key.get(0));

        for (Fields selector : columnFields) {
            TupleEntry values = tupleEntry.selectEntry(selector);

            for (int j = 0; j < values.getFields().size(); j++) {
                Fields fields = values.getFields();
                Tuple tuple = values.getTuple();

                Object name = fields.get(j);
                Object value = tuple.get(j);

                Mutation mutation = createColumnPutMutation(ByteBuffer.wrap(serializer.toBytes(name)), ByteBuffer.wrap(serializer.toBytes(value)));
                outputCollector.collect(ByteBuffer.wrap(keyBytes), Collections.singletonList(mutation));
            }
        }
    }

    private Mutation createColumnPutMutation(ByteBuffer name, ByteBuffer value) {
        Column column = new Column(name);
        column.setName(name);
        column.setValue(value);
        column.setTimestamp(System.currentTimeMillis());

        Mutation m = new Mutation();
        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        columnOrSuperColumn.setColumn(column);
        m.setColumn_or_supercolumn(columnOrSuperColumn);

        return m;
    }
}
