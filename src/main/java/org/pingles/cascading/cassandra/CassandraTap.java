package org.pingles.cascading.cassandra;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;

public class CassandraTap extends Tap {
    @Override
    public String getIdentifier() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess flowProcess, Object o) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean createResource(Object o) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteResource(Object o) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean resourceExists(Object o) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getModifiedTime(Object o) throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
