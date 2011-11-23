Storm Cassandra Integration
===========================

Integrates Storm and Cassandra by providing a generic and configurable `backtype.storm.Bolt` 
implementation that writes Storm `Tuple` objects to a Cassandra Column Family.

How the Storm `Tuple` data is written to Cassandra is dynamically configurable -- you
provide classes that "determine" a column family, row key, and column name/values, and the 
bolt will write the data to a Cassandra cluster.

### Usage

**Basic Usage**

`CassandraBolt` expects that a Cassandra hostname, port, and keyspace be set in the Storm topology configuration:

		Config config = new Config();
		config.put(CassandraBolt.CASSANDRA_HOST, "localhost");
		config.put(CassandraBolt.CASSANDRA_PORT, 9160);
		config.put(CassandraBolt.CASSANDRA_KEYSPACE, "testKeyspace");
		
		
The `CassandraBolt` class provides a convenience constructor that takes a column family name, and row key field value as arguments:

		IRichBolt cassandraBolt = new CassandraBolt("columnFamily", "rowKey");

The above constructor will create a `CassandraBolt` that writes to the "`columnFamily`" column family, and will look for/use a field 
named "`rowKey`" in the `backtype.storm.tuple.Tuple` objects it receives as the Cassandra row key.

For each field in the `backtype.storm.Tuple` received, the `CassandraBolt` will write a column name/value pair.

For example, given the constructor listed above, a tuple value of:

		{rowKey: 12345, field1: "foo", field2: "bar}

Would yield the following Cassandra row (as seen from `cassandra-cli`):

		RowKey: 12345
		=> (column=field1, value=foo, timestamp=1321938505071001)
		=> (column=field2, value=bar, timestamp=1321938505072000)



