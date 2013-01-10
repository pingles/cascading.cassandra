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
import java.util.UUID;

public class CassandraTap extends Tap {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(CassandraScheme.class);
    private static final String URI_SCHEME = "cassandra";
    private static final String THRIFT_PORT_KEY = "cassandra.thrift.port";
    private static final String DEFAULT_RPC_PORT = "9160";
    private static final String DEFAULT_ADDRESS = "localhost";

    private final String initialAddress;
    private final Integer rpcPort;
    private final String pathUUID;
    private String columnFamilyName;
    private String keyspace;

    public CassandraTap(
            String keyspace, String columnFamilyName, CassandraScheme scheme) {
        super(scheme, SinkMode.UPDATE);
        this.initialAddress = null;
        this.rpcPort = null;
        this.columnFamilyName = columnFamilyName;
        this.keyspace = keyspace;
	this.pathUUID = java.util.UUID.randomUUID().toString();
    }


    public CassandraTap(
            String initialAddress, Integer rpcPort, String keyspace,
            String columnFamilyName, CassandraScheme scheme) {
        super(scheme, SinkMode.APPEND);
        this.initialAddress = initialAddress;
        this.rpcPort = rpcPort;
        this.columnFamilyName = columnFamilyName;
        this.keyspace = keyspace;
	this.pathUUID = java.util.UUID.randomUUID().toString();
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
        LOGGER.info("Created Cassandra tap {}", getPath());
        LOGGER.info("Sinking to column family: {}", columnFamilyName);

        super.sinkInit(conf);

        ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamilyName);
        ConfigHelper.setOutputPartitioner(conf,
            "org.apache.cassandra.dht.RandomPartitioner");
        sinkEndpointInit(conf);
    }

    @Override
    public void sourceInit(JobConf conf) throws IOException {
        LOGGER.info("Sourcing from column family: {}", columnFamilyName);

        FileInputFormat.addInputPaths(conf, getPath().toString());
        conf.setInputFormat(ColumnFamilyInputFormat.class);
        ConfigHelper.setInputColumnFamily(conf, keyspace, columnFamilyName);
        sourceEndpointInit(conf);

        super.sourceInit(conf);
    }

    protected void sourceEndpointInit(JobConf conf) throws IOException {
        if (initialAddress != null) {
            ConfigHelper.setInputInitialAddress(conf, initialAddress);
        } else if (ConfigHelper.getInputInitialAddress(conf) == null) {
            ConfigHelper.setInputInitialAddress(conf, DEFAULT_ADDRESS);
        }

        if (rpcPort != null) {
            ConfigHelper.setInputRpcPort(conf, rpcPort.toString());
        } else if (conf.get(THRIFT_PORT_KEY) == null) {
            ConfigHelper.setInputRpcPort(conf, DEFAULT_RPC_PORT);
        }
    }
    
    protected void sinkEndpointInit(JobConf conf) throws IOException {
        if (initialAddress != null) {
            ConfigHelper.setOutputInitialAddress(conf, initialAddress);
        } else if (ConfigHelper.getOutputInitialAddress(conf) == null) {
            ConfigHelper.setOutputInitialAddress(conf, DEFAULT_ADDRESS);
        }

        if (rpcPort != null) {
            ConfigHelper.setOutputRpcPort(conf, rpcPort.toString());
        } else if (conf.get(THRIFT_PORT_KEY) == null) {
            ConfigHelper.setOutputRpcPort(conf, DEFAULT_RPC_PORT);
        }
    }


    protected String getStringURI() {
        String host = (initialAddress != null)
            ? String.format("%s:%s", initialAddress, rpcPort) : "";
        return String.format("%s://%s/%s/%s",
            URI_SCHEME, host, keyspace, columnFamilyName);
    }

    @Override
    public Path getPath() {
        return new Path(pathUUID);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[\"" + URI_SCHEME + "\"]"
            + "[\"" + Util.sanitizeUrl(getStringURI()) + "\"]"
            + "[\"" + pathUUID + "\"]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        CassandraTap tap = (CassandraTap) obj;
        if (!getScheme().equals(tap.getScheme())) return false;
        return getStringURI().equals(tap.getStringURI());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getStringURI().hashCode();
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
