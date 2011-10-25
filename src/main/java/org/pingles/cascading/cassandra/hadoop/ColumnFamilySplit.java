package org.pingles.cascading.cassandra.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ColumnFamilySplit implements InputSplit, Writable {
    private String startToken;
    private String endToken;
    private String[] dataNodes;

    public ColumnFamilySplit(String startToken, String endToken, String[] dataNodes) {
        assert startToken != null;
        assert endToken != null;
        this.startToken = startToken;
        this.endToken = endToken;
        this.dataNodes = dataNodes;
    }

    public String getStartToken() {
        return startToken;
    }

    public String getEndToken() {
        return endToken;
    }

    public long getLength() throws IOException {
        return Long.MAX_VALUE;
    }

    public String[] getLocations() throws IOException {
        return dataNodes;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(startToken);
        out.writeUTF(endToken);

        out.writeInt(dataNodes.length);
        for (String endpoint : dataNodes) {
            out.writeUTF(endpoint);
        }

    }

    public void readFields(DataInput in) throws IOException {
        startToken = in.readUTF();
        endToken = in.readUTF();

        int numOfEndpoints = in.readInt();
        dataNodes = new String[numOfEndpoints];
        for(int i = 0; i < numOfEndpoints; i++)
        {
            dataNodes[i] = in.readUTF();
        }
    }

    private String dataNodeString() {
        StringBuffer sb = new StringBuffer();
        for (String n : dataNodes) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(n);
        }
        return sb.toString();
    }

    public static ColumnFamilySplit read(DataInput in) throws IOException
    {
        ColumnFamilySplit w = new ColumnFamilySplit();
        w.readFields(in);
        return w;
    }

    // This should only be used by KeyspaceSplit.read();
    protected ColumnFamilySplit() {}

    @Override
    public String toString() {
        return "ColumnFamilySplit{" +
               "startToken='" + startToken + '\'' +
               ", endToken='" + endToken + '\'' +
               ", dataNodes=" + dataNodeString() +
               '}';
    }

}
