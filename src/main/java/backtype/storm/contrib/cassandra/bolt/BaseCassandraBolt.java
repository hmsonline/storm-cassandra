package backtype.storm.contrib.cassandra.bolt;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.RowKeyDeterminable;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public abstract class BaseCassandraBolt implements CassandraConstants {

	private static final Logger LOG = LoggerFactory
			.getLogger(BaseCassandraBolt.class);

	private String cassandraHost;
	private String cassandraPort;
	private String cassandraKeyspace;

	protected Cluster cluster;
	protected Keyspace keyspace;

	// protected OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context) {
		// LOG.debug("Preparing...");
		this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
		this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
		this.cassandraPort = String.valueOf(stormConf.get(CASSANDRA_PORT));
		initCassandraConnection();

		// this.collector = collector;
	}

	private void initCassandraConnection() {
		try {
			AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
					.forCluster("ClusterName")
					.forKeyspace(this.cassandraKeyspace)
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl()
									.setDiscoveryType(NodeDiscoveryType.NONE))
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(
									"MyConnectionPool")
									.setPort(
											Integer.parseInt(this.cassandraPort))
									.setMaxConnsPerHost(1)
									.setSeeds(
											this.cassandraHost + ":"
													+ this.cassandraPort))
					.withConnectionPoolMonitor(
							new CountingConnectionPoolMonitor())
					.buildKeyspace(ThriftFamilyFactory.getInstance());

			context.start();
			this.keyspace = context.getEntity();
		} catch (Throwable e) {
			LOG.warn("Preparation failed.", e);
			throw new IllegalStateException("Failed to prepare CassandraBolt",
					e);
		}
	}

	public void writeTuple(Tuple input, ColumnFamilyDeterminable cfDeterminer,
			RowKeyDeterminable rkDeterminer) throws ConnectionException {
		String columnFamilyName = cfDeterminer.determineColumnFamily(input);
		String rowKey = (String) rkDeterminer.determineRowKey(input);
		MutationBatch mutation = keyspace.prepareMutationBatch();
		ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(
				columnFamilyName, StringSerializer.get(),
				StringSerializer.get());
		this.addTupleToMutation(input, columnFamily, rowKey, mutation);
		mutation.execute();
	}

	public void writeTuples(List<Tuple> inputs,
			ColumnFamilyDeterminable cfDeterminer,
			RowKeyDeterminable rkDeterminer) throws ConnectionException {
		MutationBatch mutation = keyspace.prepareMutationBatch();
		for (Tuple input : inputs) {
			String columnFamilyName = cfDeterminer.determineColumnFamily(input);
			String rowKey = (String) rkDeterminer.determineRowKey(input);
			ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(
					columnFamilyName, StringSerializer.get(),
					StringSerializer.get());
			this.addTupleToMutation(input, columnFamily, rowKey, mutation);
		}
		mutation.execute();
	}
	
	private void addTupleToMutation(Tuple input,
			ColumnFamily<String, String> columnFamily, String rowKey,
			MutationBatch mutation) {
		Fields fields = input.getFields();
		for (int i = 0; i < fields.size(); i++) {
			mutation.withRow(columnFamily, rowKey);
		}
	}

}
