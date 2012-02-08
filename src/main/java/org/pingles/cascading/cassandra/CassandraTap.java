package org.pingles.cascading.cassandra;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.pingles.cascading.cassandra.hadoop.ColumnFamilyInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class CassandraTap extends Tap {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(CassandraScheme.class);
    private static final String URI_SCHEME = "cassandra";

    private final String initialAddress;
    private final Integer rpcPort;
    private String columnFamilyName;
    private String keyspace;

    public CassandraTap(
            String initialAddress, Integer rpcPort, String keyspace,
            String columnFamilyName, CassandraScheme scheme) {
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
        ConfigHelper.setPartitioner(conf,
            "org.apache.cassandra.dht.RandomPartitioner");
        ConfigHelper.setInitialAddress(conf, this.initialAddress);
        ConfigHelper.setRpcPort(conf, this.rpcPort.toString());
    }

    @Override
    public void sourceInit(JobConf conf) throws IOException {
        LOGGER.info("Sourcing from column family: {}", columnFamilyName);

        FileInputFormat.addInputPaths(conf, getPath().toString());
        conf.setInputFormat(ColumnFamilyInputFormat.class);
        ConfigHelper.setInputColumnFamily(conf, keyspace, columnFamilyName);
        ConfigHelper.setInitialAddress(conf, initialAddress);
        ConfigHelper.setRpcPort(conf, rpcPort.toString());

        super.sourceInit(conf);
    }

    protected String getStringPath() {
        return String.format("%s://%s:%d/%s/%s",
            URI_SCHEME, initialAddress, rpcPort, keyspace, columnFamilyName);
    }

    @Override
    public Path getPath() {
        return new Path(getStringPath());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[\"" + URI_SCHEME + "\"]"
            + "[\"" + Util.sanitizeUrl(getStringPath()) + "\"]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        CassandraTap tap = (CassandraTap) obj;
        if (!getScheme().equals(tap.getScheme())) return false;
        return getStringPath().equals(tap.getStringPath());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getStringPath().hashCode();
        return result;
    }

    @Override
    public TupleEntryIterator openForRead(JobConf jobConf) throws IOException {
        return new TupleEntryIterator(
            getSourceFields(), new TapIterator(this, jobConf));
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf jobConf) throws IOException {
        return new TapCollector(this, jobConf);
    }

    @Override
    public boolean makeDirs(JobConf jobConf) throws IOException {
        throw new UnsupportedOperationException(
            "makeDirs unsupported with Cassandra.");
    }

    @Override
    public boolean deletePath(JobConf jobConf) throws IOException {
        throw new UnsupportedOperationException(
            "deletePath unsupported with Cassandra.");
    }

    @Override
    public boolean pathExists(JobConf jobConf) throws IOException {
        throw new UnsupportedOperationException(
            "pathExists unsupported with Cassandra.");
    }

    @Override
    public long getPathModified(JobConf jobConf) throws IOException {
        throw new UnsupportedOperationException(
            "getPathModified unsupported with Cassandra.");
    }
}
