package org.pingles.cascading.cassandra;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CassandraTap extends Tap {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheme.class);
    private static final String SCHEME = "cassandra";

    private final String initialAddress;
    private final Integer rpcPort;
    private String columnFamilyName;
    private String keyspace;

    public CassandraTap(String initialAddress, Integer rpcPort, String keyspace, String columnFamilyName, CassandraScheme scheme) {
        super(scheme, SinkMode.APPEND);
        this.initialAddress = initialAddress;
        this.rpcPort = rpcPort;
        this.columnFamilyName = columnFamilyName;
        this.keyspace = keyspace;
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
        LOGGER.info("Created Cassandra tap {}", getPath());
        LOGGER.info("Sinking to column family: {}", columnFamilyName);

        super.sinkInit(conf);

        ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamilyName);
        ConfigHelper.setPartitioner(conf, "org.apache.cassandra.dht.RandomPartitioner");
        ConfigHelper.setInitialAddress(conf, this.initialAddress);
        ConfigHelper.setRpcPort(conf, this.rpcPort.toString());
    }

    @Override
    public void sourceInit(JobConf conf) throws IOException {
        LOGGER.info("Sourcing from column family: {}", columnFamilyName);
        super.sourceInit(conf);
    }

    @Override
    public Path getPath() {
        return new Path(String.format("%s://%s:%d/%s/%s", SCHEME, initialAddress, rpcPort, keyspace, columnFamilyName));
    }

    @Override
    public TupleEntryIterator openForRead(JobConf jobConf) throws IOException {
        return new TupleEntryIterator(getSourceFields(), new TapIterator(this, jobConf));
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf jobConf) throws IOException {
        return new TapCollector(this, jobConf);
    }

    @Override
    public boolean makeDirs(JobConf jobConf) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean deletePath(JobConf jobConf) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean pathExists(JobConf jobConf) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public long getPathModified(JobConf jobConf) throws IOException {
        throw new NotImplementedException();
    }
}
