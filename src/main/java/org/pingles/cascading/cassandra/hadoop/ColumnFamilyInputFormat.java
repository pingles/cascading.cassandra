package org.pingles.cascading.cassandra.hadoop;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.System.nanoTime;

public class ColumnFamilyInputFormat implements InputFormat<ByteBuffer, SortedMap<ByteBuffer, IColumn>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnFamilyInputFormat.class);
    private String keyspace;
    private String cfName;

    private void validateConfiguration(JobConf conf) {
        if (ConfigHelper.getInputKeyspace(conf) == null) {
            throw new UnsupportedOperationException("you must set the keyspace with setColumnFamily()");
        }
        if (ConfigHelper.getInputColumnFamily(conf) == null) {
            throw new UnsupportedOperationException("you must set the column family with setColumnFamily()");
        }
        if (ConfigHelper.getInputSlicePredicate(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setPredicate");
        }

    }

    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        validateConfiguration(jobConf);

        List<TokenRange> masterRangeNodes = getRangeMap(jobConf);
        keyspace = ConfigHelper.getInputKeyspace(jobConf);
        cfName = ConfigHelper.getInputColumnFamily(jobConf);

        ExecutorService executor = Executors.newCachedThreadPool();
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

        try {
            List<Future<List<InputSplit>>> splitFutures = new ArrayList<Future<List<InputSplit>>>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(jobConf);
            IPartitioner partitioner = null;
            Range jobRange = null;

            if (jobKeyRange != null) {
                partitioner = ConfigHelper.getPartitioner(jobConf);
                assert partitioner.preservesOrder() : "ConfigHelper.setInputKeyRange(..) can only be used with a order preserving paritioner";
                assert jobKeyRange.start_key == null : "only start_token supported";
                assert jobKeyRange.end_key == null : "only end_token supported";
                jobRange = new Range(partitioner.getTokenFactory().fromString(jobKeyRange.start_token), partitioner.getTokenFactory().fromString(jobKeyRange.end_token), partitioner);
            }

            for (TokenRange range : masterRangeNodes) {
                if (jobRange == null) {
                    splitFutures.add(executor.submit(new SplitCallable(range, jobConf)));
                }
                if (jobRange != null) {
                    Range dhtRange = new Range(partitioner.getTokenFactory().fromString(range.start_token),
                                               partitioner.getTokenFactory().fromString(range.end_token),
                                               partitioner);

                    if (dhtRange.intersects(jobRange)) {
                        for (Range intersection: dhtRange.intersectionWith(jobRange))
                        {
                            range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                            range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                            // for each range, pick a live owner and ask it to compute bite-sized splits
                            splitFutures.add(executor.submit(new SplitCallable(range, jobConf)));
                        }
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<InputSplit>> futureInputSplits : splitFutures) {
                try {
                    splits.addAll(futureInputSplits.get());
                } catch (Exception e) {
                    throw new IOException("Could not get input splits", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(nanoTime()));

        LOGGER.info("Splits are: {}", StringUtils.join(splits, ","));

        return splits.toArray(new InputSplit[splits.size()]);
    }

    private List<TokenRange> getRangeMap(JobConf jobConf) throws IOException {
        Cassandra.Client client = getClientFromAddressList(jobConf);

        List<TokenRange> map;
        try {
            map = client.describe_ring(ConfigHelper.getInputKeyspace(jobConf));
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new ColumnFamilyRecordReader((ColumnFamilySplit) inputSplit, jobConf);
    }

    public static Cassandra.Client getClientFromAddressList(JobConf jobConf) throws IOException
    {
        String[] addresses = ConfigHelper.getInitialAddress(jobConf).split(",");
        Cassandra.Client client = null;
        List<IOException> exceptions = new ArrayList<IOException>();
        for (String address : addresses) {
            try {
                client = createConnection(address, ConfigHelper.getRpcPort(jobConf), true);
                break;
            } catch (IOException ioe) {
                exceptions.add(ioe);
            }
        }
        if (client == null) {
            LOGGER.error("failed to connect to any initial addresses");
            for (IOException ioe : exceptions) {
                LOGGER.error("", ioe);
            }
            throw exceptions.get(exceptions.size() - 1);
        }
        return client;
    }

    public static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws IOException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try {
            trans.open();
        } catch (TTransportException e) {
            throw new IOException("unable to connect to server", e);
        }
        return new Cassandra.Client(new TBinaryProtocol(trans));
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<InputSplit>>
    {

        private final TokenRange range;
        private final JobConf conf;

        public SplitCallable(TokenRange tr, JobConf conf) {
            this.range = tr;
            this.conf = conf;
        }

        public List<InputSplit> call() throws Exception {
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            List<String> tokens = getSubSplits(keyspace, cfName, range, conf);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (String endpoint: range.rpc_endpoints) {
                String endpoint_address = endpoint;
		        if (endpoint_address == null || endpoint_address.equals("0.0.0.0"))
			        endpoint_address = range.endpoints.get(endpointIndex);
		        endpoints[endpointIndex++] = InetAddress.getByName(endpoint_address).getHostName();
            }

            for (int i = 1; i < tokens.size(); i++) {
                ColumnFamilySplit split = new ColumnFamilySplit(tokens.get(i - 1), tokens.get(i), endpoints);
                LOGGER.debug("adding " + split);
                splits.add(split);
            }
            return splits;
        }
    }

    private List<String> getSubSplits(String keyspace, String cfName, TokenRange range, JobConf conf) throws IOException {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (String host : range.rpc_endpoints) {
            try {
                Cassandra.Client client = createConnection(host, ConfigHelper.getRpcPort(conf), true);
                client.set_keyspace(keyspace);
                return client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
            } catch (IOException e) {
                LOGGER.debug("failed connect to endpoint " + host, e);
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("failed connecting to all endpoints " + StringUtils.join(range.endpoints, ","));
    }
}
