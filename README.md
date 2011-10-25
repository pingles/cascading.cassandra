# cascading.cassandra

A [Cascading](http://www.cascading.org) Scheme and Tap for [Cassandra](http://cassandra.apache.org).

## Installing

The project can be built using [Maven](http://maven.apache.org) and installed into your local repository:

    mvn install

## Restrictions

This is very much an early Work-in-Progress. The Tap can only be used as a sink and only supports regular column families (it won't work with Super-Columns or Counter Columns).

## Usage

`cascading.cassandra` is heavily influenced by [cascading.hbase](https://github.com/cwensel/cascading.hbase) and uses Cassandra's built-in `ColumnFamilyOutputFormat`.

First, create a `CassandraScheme` and specify the field to be used as the row key (currently the Scheme will only support single-field keys). The second parameter is an array of Fields that represent the columns you wish to store. The column names will be serialized from the name provided and the values will come from the Tuples during the flow.

    Fields keyFields = new Fields("num");
    Fields[] valueFields = new Fields[] {new Fields("lower"), new Fields("upper")};
    CassandraScheme scheme = new CassandraScheme(keyFields, valueFields);

Finally, hook the `CassandraScheme` into a `CassandraTap` and provide the Cassandra Thrift RPC Host and Port that the `ColumnFamilyOutputFormat` should connect to, as well as the keyspace and column family names you wish to store values for.

    Tap sink = new CassandraTap(getRpcHost(), getRpcPort(), keyspaceName, columnFamilyName, scheme);

## License

Licensed under the Apache 2.0 license.

## Copyright

Copyright &copy; Paul Ingles, 2011.
