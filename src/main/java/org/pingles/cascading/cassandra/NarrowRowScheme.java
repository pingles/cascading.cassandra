package org.pingles.cascading.cassandra;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
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
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class NarrowRowScheme extends CassandraScheme {
    private static final org.slf4j.Logger LOGGER =
        LoggerFactory.getLogger(NarrowRowScheme.class);
    private Fields keyField;
    private Fields nameFields;

    /**
     * Creates a {@link Scheme} suitable for using with a Sink.
     * @param keyField      the field to use for the row key
     * @param nameFields  column names
     */
    public NarrowRowScheme(Fields keyField, Fields nameFields) {
        this.keyField = keyField;
        this.nameFields = nameFields;
    }

    /**
     * Creates a {@link Scheme} suitable for using with a Source.
     * @param fields
     */
    public NarrowRowScheme(Fields fields) {
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
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
            throws IOException {
        Tuple key = tupleEntry.selectTuple(keyField);
        TypeInferringSerializer<Object> serializer = TypeInferringSerializer.get();
        ByteBuffer keyBuffer = serialize(serializer, key.get(0));

        int nfields = nameFields.size();
        List mutations = new ArrayList<Mutation>(nfields);
        for (int i = 0; i < nfields; i++) {
            Comparable name = nameFields.get(i);
            Comparable value = tupleEntry.get(name);

            Mutation mutation = createColumnPutMutation(
                serialize(serializer, name), serialize(serializer, value));
            mutations.add(mutation);
        }
        outputCollector.collect(keyBuffer, mutations);
    }
}
