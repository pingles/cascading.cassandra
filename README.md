# cascading.cassandra

A [Cascading](http://www.cascading.org) Scheme and Tap for [Cassandra](http://cassandra.apache.org) and can operate as both a Sink and a Source.

Current build status: ![Build status](https://secure.travis-ci.org/pingles/cascading.cassandra.png)

## Installing

The project can be built using [Maven](http://maven.apache.org) and installed into your local repository:

    mvn install

Alternatively, there's a snapshot available from [Conjars](http://conjars.org/cascading.pingles/cascading.cassandra).

### Leiningen

    [cascading.cassandra/cascading.cassandra "0.0.5"]
    
and add the repository into your `project.clj` with

    :repositories {"conjars" "http://conjars.org/repo"}

### Maven

    <dependency>
      <groupId>cascading.cassandra</groupId>
      <artifactId>cascading.cassandra</artifactId>
      <version>0.0.5</version>
    </dependency>

If you haven't already added it you'll need to add the Conjars repository

    <repositories>
      <repository>
        <id>conjars</id>
        <url>http://conjars.org/repo</url>
      </repository>
    </repositories>


## Restrictions

This is very much an early Work-in-Progress and all contributions are welcome. It only supports regular column families currently (no super columns or counter columns yet).

## Usage

`cascading.cassandra` is heavily influenced by [cascading.hbase](https://github.com/cwensel/cascading.hbase) and uses Cassandra's `ColumnFamilyOutputFormat` for it's sink.

First, create a `CassandraScheme` and specify the field to be used as the row key (currently the Scheme will only work with single-field keys). The second parameter is an array of Fields that represent the columns you wish to store. The column names will be serialized from the name provided and the values will come from the Tuples during the flow.

### Using as a Sink

For narrow rows with known column names, use the `NarrowRowScheme`, specifying the field to use for the key and fields to use a column values.  The field names are used for the written column names:

    Fields keyFields = new Fields("num");
    Fields nameFields = new Fields("lower", "upper");
    CassandraScheme scheme = new NarrowRowScheme(keyFields, nameFields);

Finally, hook the `CassandraScheme` into a `CassandraTap` and provide the Cassandra Thrift RPC Host and Port that the `ColumnFamilyOutputFormat` should connect to, as well as the keyspace and column family names you wish to store/retrieve values for.

    Tap sink = new CassandraTap(getRpcHost(), getRpcPort(), keyspaceName, columnFamilyName, scheme);

For dumping wide rows, use the `WideRowScheme`, which takes no argument in construction:

    CassandraScheme scheme = new WideRowScheme();

This scheme expects each sunk tuple to consist of a row key followed by any number of column name / value pairs.

### Using as a Source

Using the `NarrowRowScheme` with a source is identical to usage with a sink.  The `WideRowScheme` cannot currently be used as a source.

## License

Licensed under the Apache 2.0 license.

## Copyright

Copyright &copy; Paul Ingles, 2011.
