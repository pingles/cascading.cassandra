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
import org.apache.hadoop.io.BytesWritable;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.cassandra.thrift.SliceRange;

public class WideRowScheme extends CassandraScheme {
    private static final org.slf4j.Logger LOGGER =
        LoggerFactory.getLogger(WideRowScheme.class);

    private int maxColumns = 5000;
    /**
     * Creates a {@link Scheme} suitable for using with a Sink.
     */
    public WideRowScheme() {
    }

    public WideRowScheme(int maxColumns) {
    	this.maxColumns = maxColumns;
    }
    
	public void setMaxColumns(int maxColumns) {
		this.maxColumns = maxColumns;
	}

	@Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
		
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();

        SlicePredicate predicate = new SlicePredicate();

        SliceRange sliceRange = new SliceRange();
        sliceRange.start = ByteBuffer.allocate(0);
        sliceRange.finish = ByteBuffer.allocate(0);
        sliceRange.reversed = false;
        sliceRange.count = maxColumns;
        predicate.setSlice_range( sliceRange );
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
    	
        Tuple tuple = new Tuple();
        SortedMap<ByteBuffer, IColumn> values =
            (SortedMap<ByteBuffer, IColumn>) value;

        tuple.add((ByteBuffer) key );
        
        Set<ByteBuffer> keySets = values.keySet();
        for(ByteBuffer name : keySets) {
        	IColumn v = values.get(name);
        	tuple.add( (ByteBuffer) name );
            if (v != null) {
                tuple.add(v.value());
            } else {
                tuple.add(null);
            }
        }

        return tuple;
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
          throws IOException {
        Tuple tuple = tupleEntry.getTuple();
        TypeInferringSerializer<Object> serializer = TypeInferringSerializer.get();
        ByteBuffer keyBuffer = serialize(serializer, tuple.get(0));

        int nvalues = tuple.size();
        List mutations = new ArrayList<Mutation>(nvalues);
        for (int i = 1; i < nvalues; i += 2) {
            Comparable name = tuple.get(i);
            Comparable value = tuple.get(i+1);

            Mutation mutation = createColumnPutMutation(
                serialize(serializer, name), serialize(serializer, value));
            mutations.add(mutation);
        }
        outputCollector.collect(keyBuffer, mutations);
    }
}
