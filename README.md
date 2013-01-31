Storm Cassandra Integration
===========================

Integrates Storm and Cassandra by providing a generic and configurable `backtype.storm.Bolt` 
implementation that writes Storm `Tuple` objects to a Cassandra Column Family.

How the Storm `Tuple` data is written to Cassandra is dynamically configurable -- you
provide classes that "determine" a column family, row key, and column name/values, and the 
bolt will write the data to a Cassandra cluster.

### Project Location
Primary development of storm-cassandra will take place at: 
https://github.com/ptgoetz/storm-cassandra

Point/stable (non-SNAPSHOT) release souce code will be pushed to:
https://github.com/nathanmarz/storm-contrib

Maven artifacts for releases will be available on maven central.

### Building from Source

		$ mvn install

### Usage

**Basic Usage**

`CassandraBolt`, `TridentCassandraLookupFunction`, and `TridentCassandraWriteFunction` expects that a Cassandra hostname, 
port, and keyspace be set in the Storm topology configuration.  To allow for multiple instances of these in a topology
and not require that they all connect to the same Cassandra instance the values are added to the Storm configuration
using a key and a map.  The key to indicate which map to use is set in the constructor of these classes when instantiating
them.

		Map<String, Object> cassandraConfig = new HashMap<String, Object>();
		cassandraConfig.put(CassandraBolt.CASSANDRA_HOST, "localhost:9160");
		cassandraConfig.put(CassandraBolt.CASSANDRA_KEYSPACE, "testKeyspace");
		Config config = new Config();
		config.put("CassandraLocal", cassandraConfig);
		
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
		
**Cassandra Write Function**
Storm Trident filters out the original Tuple if a function doesn't emit anything.  To allow for additional processing after
writing to Cassandra the `TridentCassandraWriteFunction` can emit a static Object value.  The main purpose for this emit is
to simply allow the Tuple to continue as opposed to filtering it out.  The static value can be set in either the constructor
or by calling the setValueToEmitAfterWrite method.  Setting the emit value to NULL will cause the function to not emit anything
and Storm will filter the Tuple out.  Default behavior is to not emit.
If the function will emit a value don't forget to declare the output field when building the topology.
		
**Cassandra Counter Columns**

The Counter Column concept is similar to the above,
however you must specify the rowKey and a value to specify the increment amount. All other fields will be assumed to specify columns to be incremented by said amount. 

		CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(
				"columnFamily", "RowKeyField", "IncrementAmountField" );
				
The above constructor will create a bolt that writes to the "`columnFamily`" column family, and will use a field named "`RowKeyField`"
in the tuples that it receives. All remaining fields in the Tuple will be assumed to contain the names of the columns to be incremented.

Given the following Tuple:

		{rowKey: 12345, IncrementAmount: 1L, IncrementColumn: 'SomeCounter'}
		
Would increment the "`SomeCounter`" counter column by 1L.


# Examples
The "examples" directory contains two examples:

* CassandraReachTopology

* PersistentWordCount

## Cassandra Reach Topology

The [`CassandraReachTopology`](https://github.com/ptgoetz/storm-cassandra/blob/master/examples/src/main/java/backtype/storm/contrib/cassandra/example/CassandraReachTopology.java) 
example is a Storm [Distributed RPC](https://github.com/nathanmarz/storm/wiki/Distributed-RPC) example 
that is essentially a clone of Nathan Marz' [`ReachTopology`](https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/ReachTopology.java), 
that instead of using in-memory data stores  is backed by a [Cassandra](http://cassandra.apache.org/) database and uses generic 
*storm-cassandra* bolts to query the database.

## Persistent Word Count  
The sample [`PersistentWordCount`](https://github.com/ptgoetz/storm-cassandra/blob/master/examples/src/main/java/backtype/storm/contrib/cassandra/example/PersistentWordCount.java) 
topology illustrates the basic usage of the Cassandra Bolt implementation. It reuses the [`TestWordSpout`](https://github.com/nathanmarz/storm/blob/master/src/jvm/backtype/storm/testing/TestWordSpout.java) 
spout and [`TestWordCounter`](https://github.com/nathanmarz/storm/blob/master/src/jvm/backtype/storm/testing/TestWordCounter.java) 
bolt from the Storm tutorial, and adds an instance of `CassandraBolt` to persist the results.


## Preparation
In order to run the examples, you will need a Cassandra database running on `localhost:9160`.

### Build the Example Source

		$ cd examples
		$ mvn install
	
### Create Cassandra Schema and Sample Data
Install and run [Apache Cassandra](http://cassandra.apache.org/).

Create the sample schema using `cassandra-cli`:

		$ cd schema
		$ cat cassandra_schema.txt | cassandra-cli -h localhost

## Running the Cassandra Reach Topology

To run the `CassandraReachTopology` execute the following maven command:

		$ mvn exec:java -Dexec.mainClass=com.hmsonline.storm.cassandra.example.CassandraReachTopology

Among the output, you should see the following:

	Reach of http://github.com/hmsonline: 3
	Reach of http://github.com/nathanmarz: 3
	Reach of http://github.com/ptgoetz: 4
	Reach of http://github.com/boneill: 0

To enable logging of all tuples sent within the topology, run the following command:

		$ mvn exec:java -Dexec.mainClass=com.hmsonline.storm.cassandra.example.CassandraReachTopology -Ddebug=true


## Running the Persistent Word Count Example

The `PersistentWordCount` example build the following topology:

	TestWordSpout ==> TestWordCounter ==> CassandraBolt
	
**Data Flow**

1. `TestWordSpout` emits words at random from a pre-defined list.
2. `TestWordCounter` receives a word, updates a counter for that word,
and emits a tuple containing the word and corresponding count ("word", "count").
3. The `CassandraBolt` receives the ("word", "count") tuple and writes it to the
Cassandra database using the word as the row key.


Run the `PersistentWordCount` topology:

		$ mvn exec:java -Dexec.mainClass=com.hmsonline.storm.cassandra.example.PersistentWordCount
	
View the end result in `cassandra-cli`:

		$ cassandra-cli -h localhost
		[default@unknown] use stormks;
		[default@stromks] list stormcf;
	
The output should resemble the following:

		Using default limit of 100
		-------------------
		RowKey: nathan
		=> (column=count, value=22, timestamp=1322332601951001)
		=> (column=word, value=nathan, timestamp=1322332601951000)
		-------------------
		RowKey: mike
		=> (column=count, value=11, timestamp=1322332600330001)
		=> (column=word, value=mike, timestamp=1322332600330000)
		-------------------
		RowKey: jackson
		=> (column=count, value=17, timestamp=1322332600633001)
		=> (column=word, value=jackson, timestamp=1322332600633000)
		-------------------
		RowKey: golda
		=> (column=count, value=31, timestamp=1322332602155001)
		=> (column=word, value=golda, timestamp=1322332602155000)
		-------------------
		RowKey: bertels
		=> (column=count, value=16, timestamp=1322332602257000)
		=> (column=word, value=bertels, timestamp=1322332602255000)
		
		5 Rows Returned.
		Elapsed time: 8 msec(s).

