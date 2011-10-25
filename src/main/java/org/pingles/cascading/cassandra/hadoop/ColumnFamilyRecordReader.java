package org.pingles.cascading.cassandra.hadoop;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class ColumnFamilyRecordReader implements RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> {
    private final ColumnFamilySplit inputSplit;
    private final JobConf jobConf;
    private final SlicePredicate predicate;
    private final int totalRowCount;
    private final int batchRowCount;
    private final String columnFamily;
    private final ConsistencyLevel consistencyLevel;
    private final String keyspace;
    private final String initialAddress;
    private TSocket socket;
    private Cassandra.Client client;
    private Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> currentRow;
    private int rpcPort;
    private ColumnFamilyRecordReader.RowIterator rowIterator;

    public ColumnFamilyRecordReader(ColumnFamilySplit inputSplit, JobConf jobConf) {
        this.inputSplit = inputSplit;
        this.jobConf = jobConf;

        this.predicate = ConfigHelper.getInputSlicePredicate(jobConf);
        this.totalRowCount = ConfigHelper.getInputSplitSize(jobConf);
        this.batchRowCount = ConfigHelper.getRangeBatchSize(jobConf);
        this.columnFamily = ConfigHelper.getInputColumnFamily(jobConf);
        this.consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(jobConf));
        this.keyspace = ConfigHelper.getInputKeyspace(jobConf);
        this.initialAddress = ConfigHelper.getInitialAddress(jobConf);
        this.rpcPort = ConfigHelper.getRpcPort(jobConf);

        try {
            this.client = createClient();
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (AuthorizationException e) {
            throw new RuntimeException(e);
        } catch (AuthenticationException e) {
            throw new RuntimeException(e);
        }

        this.rowIterator = new RowIterator();
    }

    private TSocket getSocket() throws TTransportException {
        if (socket != null && socket.isOpen()) {
            return socket;
        }
        socket = new TSocket(initialAddress, rpcPort);
        socket.open();
        return socket;
    }

    private Cassandra.Client createClient() throws TException, InvalidRequestException, AuthorizationException, AuthenticationException {
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(new TFramedTransport(getSocket()));
        Cassandra.Client c = new Cassandra.Client(binaryProtocol);

        c.set_keyspace(keyspace);

        if (ConfigHelper.getInputKeyspaceUserName(jobConf) != null) {
            Map<String, String> creds = new HashMap<String, String>();
            creds.put("username", ConfigHelper.getInputKeyspaceUserName(jobConf));
            creds.put("password", ConfigHelper.getInputKeyspacePassword(jobConf));
            AuthenticationRequest authRequest = new AuthenticationRequest(creds);
            c.login(authRequest);
        }

        return c;
    }

    public boolean next(ByteBuffer byteBuffer, SortedMap<ByteBuffer, IColumn> byteBufferIColumnSortedMap) throws IOException {
        if (!rowIterator.hasNext())
            return false;
        currentRow = rowIterator.next();
        return true;
    }

    public ByteBuffer createKey() {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    private SortedMap<ByteBuffer, IColumn> emptyMap() {
        return new TreeMap<ByteBuffer, IColumn>();
    }

    public SortedMap<ByteBuffer, IColumn> createValue() {
        return emptyMap();
    }

    public long getPos() throws IOException {
        return rowIterator.rowsRead();
    }

    public void close() throws IOException {
        if (socket != null && socket.isOpen()) {
            socket.close();
            socket = null;
            client = null;
        }
    }

    public float getProgress() throws IOException {
        return ((float)rowIterator.rowsRead()) / totalRowCount;
    }

    private class RowIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>> {
        private List<KeySlice> rows;
        private String startToken;
        private int totalRead = 0;
        private int i = 0;
        private final AbstractType comparator;
        private final AbstractType subComparator;
        private final IPartitioner partitioner;

        private RowIterator() {
            try {
                partitioner = FBUtilities.newPartitioner(client.describe_partitioner());

                // Get the Keyspace metadata, then get the specific CF metadata
                // in order to populate the sub/comparator.
                KsDef ks_def = client.describe_keyspace(keyspace);
                List<String> cfnames = new ArrayList<String>();
                for (CfDef cfd : ks_def.cf_defs)
                    cfnames.add(cfd.name);
                int idx = cfnames.indexOf(columnFamily);
                CfDef cf_def = ks_def.cf_defs.get(idx);

                comparator = TypeParser.parse(cf_def.comparator_type);
                subComparator = cf_def.subcomparator_type == null ? null : TypeParser.parse(cf_def.subcomparator_type);
            } catch (ConfigurationException e) {
                throw new RuntimeException("unable to load sub/comparator", e);
            } catch (TException e) {
                throw new RuntimeException("error communicating via Thrift", e);
            } catch (Exception e) {
                throw new RuntimeException("unable to load keyspace " + keyspace, e);
            }
        }

        private void maybeInit() {
            // check if we need another batch
            if (rows != null && i >= rows.size())
                rows = null;

            if (rows != null)
                return;

            if (startToken == null) {
                startToken = inputSplit.getStartToken();
            } else if (startToken.equals(inputSplit.getEndToken())) {
                rows = null;
                return;
            }

            KeyRange keyRange = new KeyRange(batchRowCount);
            keyRange.setStart_token(startToken);
            keyRange.setEnd_token(inputSplit.getEndToken());
            try {
                rows = client.get_range_slices(new ColumnParent(columnFamily), predicate, keyRange, consistencyLevel);

                // nothing new? reached the end
                if (rows.isEmpty()) {
                    rows = null;
                    return;
                }

                // Pre-compute the last row key, before removing empty rows
                ByteBuffer lastRowKey = rows.get(rows.size() - 1).key;

                // only remove empty rows if the slice predicate is empty
                if (isPredicateEmpty(predicate)) {
                    Iterator<KeySlice> rowsIterator = rows.iterator();
                    while (rowsIterator.hasNext())
                        if (rowsIterator.next().columns.isEmpty())
                            rowsIterator.remove();
                }

                // reset to iterate through the new batch
                i = 0;

                // prepare for the next slice to be read
                startToken = partitioner.getTokenFactory().toString(partitioner.getToken(lastRowKey));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * @return total number of rows read by this record reader
         */
        public int rowsRead() {
            return totalRead;
        }

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> computeNext() {
            maybeInit();
            if (rows == null)
                return endOfData();

            totalRead++;
            KeySlice ks = rows.get(i++);
            SortedMap<ByteBuffer, IColumn> map = new TreeMap<ByteBuffer, IColumn>(comparator);
            for (ColumnOrSuperColumn cosc : ks.columns) {
                IColumn column = unthriftify(cosc);
                map.put(column.name(), column);
            }
            return new Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>(ks.key, map);
        }

        private IColumn unthriftify(ColumnOrSuperColumn cosc) {
            if (cosc.counter_column != null)
                return unthriftifyCounter(cosc.counter_column);
            if (cosc.counter_super_column != null)
                return unthriftifySuperCounter(cosc.counter_super_column);
            if (cosc.super_column != null)
                return unthriftifySuper(cosc.super_column);
            assert cosc.column != null;
            return unthriftifySimple(cosc.column);
        }

        private IColumn unthriftifySuper(SuperColumn super_column) {
            org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(super_column.name, subComparator);
            for (Column column : super_column.columns) {
                sc.addColumn(unthriftifySimple(column));
            }
            return sc;
        }

        private IColumn unthriftifySimple(Column column) {
            return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
        }

        private IColumn unthriftifyCounter(CounterColumn column) {
            //CounterColumns read the nodeID from the System table, so need the StorageService running and access
            //to cassandra.yaml. To avoid a Hadoop needing access to yaml return a regular Column.
            return new org.apache.cassandra.db.Column(column.name, ByteBufferUtil.bytes(column.value), 0);
        }

        private IColumn unthriftifySuperCounter(CounterSuperColumn superColumn) {
            org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(superColumn.name, subComparator);
            for (CounterColumn column : superColumn.columns)
                sc.addColumn(unthriftifyCounter(column));
            return sc;
        }
    }

    private boolean isPredicateEmpty(SlicePredicate predicate) {
        if (predicate != null) {
            if (predicate.isSetColumn_names()) {
                return false;
            }
            if (predicate.getSlice_range().getStart() != null && predicate.getSlice_range().getFinish() != null) {
                return false;
            }
        }

        return true;
    }
}
