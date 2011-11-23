package org.pingles.cascading.cassandra;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.sun.source.tree.Tree;
import me.prettyprint.cassandra.serializers.TypeInferringSerializer;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class CassandraScheme extends Scheme {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CassandraScheme.class);
    private Fields keyField;
    private Fields[] columnFields;
    private Fields nameFields;

    /**
     * Creates a {@link Scheme} suitable for using with a Sink.
     * @param keyField      the field to use for the row key
     * @param columnFields  column names
     */
    public CassandraScheme(Fields keyField, Fields[] columnFields) {
        this.keyField = keyField;
        this.columnFields = columnFields;
    }

    /**
     * Creates a {@link Scheme} suitable for using with a Source.
     * @param fields
     */
    public CassandraScheme(Fields fields) {
        this.nameFields = fields;
    }

    @Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();

        for (int i = 0; i < nameFields.size(); i++) {
            Object columnName = nameFields.get(i);
            LOGGER.info("Adding input column name: {}", columnName);
            columnNames.add(TypeInferringSerializer.get().toByteBuffer(columnName));
        }
        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(columnNames);
        ConfigHelper.setInputSlicePredicate(jobConf, predicate);
    }

    @Override
    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
        jobConf.setOutputKeyClass(ByteBuffer.class);
        jobConf.setOutputValueClass(Mutation.class);
        jobConf.setOutputFormat(ColumnFamilyOutputFormat.class);
    }

    @Override
    public Tuple source(Object key, Object value) {
        Tuple t = new Tuple();
        SortedMap<ByteBuffer, IColumn> values = (SortedMap<ByteBuffer, IColumn>) value;

        for (IColumn col : values.values()) {
            try {
                LOGGER.info("n: {}", ByteBufferUtil.string(col.name()));
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
            t.add(col.value());
        }

        return t;
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
