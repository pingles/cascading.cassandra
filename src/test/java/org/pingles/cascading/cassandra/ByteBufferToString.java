package org.pingles.cascading.cassandra;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

public class ByteBufferToString extends Identity {

    public ByteBufferToString(Fields fields) {
        super(fields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        TupleEntryCollector outputCollector = functionCall.getOutputCollector();

        Tuple tuple = arguments.getTuple();
        String[] dst = new String[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            ByteBuffer object = (ByteBuffer) tuple.getObject(i);
            try {
                dst[i] = ByteBufferUtil.string(object);
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        outputCollector.add(new Tuple(dst));
    }
}
