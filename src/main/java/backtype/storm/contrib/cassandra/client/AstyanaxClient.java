package backtype.storm.contrib.cassandra.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.TupleCounterMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author tgoetz
 *
 */
public class AstyanaxClient implements CassandraClient {
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxClient.class);
    public static final String CASSANDRA_CLUSTER_NAME = "cassandra.clusterName";
    public static final String ASTYANAX_CONFIGURATION = "astyanax.configuration";
    public static final String ASTYANAX_CONNECTION_POOL_CONFIGURATION = "astyanax.connectionPoolConfiguration";
    public static final String ASTYANAX_CONNECTION_POOL_MONITOR = "astyanax.connectioPoolMonitor";
    private AstyanaxContext<Keyspace> astyanaxContext;
    protected Cluster cluster;
    protected Keyspace keyspace;
    // not static since we're carting instances around and do not want to share them
    // between bolts
    private final Map<String,Object> DEFAULTS = new ImmutableMap.Builder<String, Object>()
            .put(CASSANDRA_CLUSTER_NAME, "ClusterName")
            .put(ASTYANAX_CONFIGURATION, new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
            .put(ASTYANAX_CONNECTION_POOL_CONFIGURATION, new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1))
            .put(ASTYANAX_CONNECTION_POOL_MONITOR, new CountingConnectionPoolMonitor())
            .build();

    protected AstyanaxContext<Keyspace> createContext(String cassandraHost, String cassandraKeyspace,
                                                      Map<String, Object> stormConfig) {
        Map<String, Object> settings = Maps.newHashMap();
        for (Map.Entry<String, Object> defaultEntry: DEFAULTS.entrySet()) {
            if (stormConfig.containsKey(defaultEntry.getKey())) {
                settings.put(defaultEntry.getKey(), stormConfig.get(defaultEntry.getKey()));
            } else {
                settings.put(defaultEntry.getKey(), defaultEntry.getValue());
            }
        }
        // in the defaults case, we don't know the seed hosts until context creation time
        if (settings.get(ASTYANAX_CONNECTION_POOL_CONFIGURATION) instanceof ConnectionPoolConfigurationImpl) {
            ConnectionPoolConfigurationImpl cpConfig = (ConnectionPoolConfigurationImpl) settings.get(ASTYANAX_CONNECTION_POOL_CONFIGURATION);
            cpConfig.setSeeds(cassandraHost);
        }

        return new AstyanaxContext.Builder()
                .forCluster((String) settings.get(CASSANDRA_CLUSTER_NAME))
                .forKeyspace(cassandraKeyspace)
                .withAstyanaxConfiguration((AstyanaxConfiguration)settings.get(ASTYANAX_CONFIGURATION))
                .withConnectionPoolConfiguration((ConnectionPoolConfiguration)settings.get(ASTYANAX_CONNECTION_POOL_CONFIGURATION))
                .withConnectionPoolMonitor((ConnectionPoolMonitor)settings.get(ASTYANAX_CONNECTION_POOL_MONITOR))
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    /* (non-Javadoc)
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#start(java.lang.String, java.lang.String)
     */
    @Override
    public void start(String cassandraHost, String cassandraKeyspace,
                      Map<String,Object> stormConfig) {
        try {
            this.astyanaxContext = createContext(cassandraHost, cassandraKeyspace, stormConfig);

            this.astyanaxContext.start();
            this.keyspace = this.astyanaxContext.getEntity();
            // test the connection
            this.keyspace.describeKeyspace();
        } catch (Throwable e) {
            LOG.warn("Astyanax initialization failed.", e);
            throw new IllegalStateException("Failed to prepare Astyanax", e);
        }
    }

    /* (non-Javadoc)
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#stop()
     */
    @Override
    public void stop() {
        this.astyanaxContext.shutdown();
    }

    /* (non-Javadoc)
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#lookup(java.lang.String, java.lang.String)
     */
    @Override
    public Map<String, String> lookup(String columnFamilyName, String rowKey) throws Exception {
        HashMap<String, String> colMap = new HashMap<String, String>();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        OperationResult<ColumnList<String>> result;
        result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();

        ColumnList<String> columns = result.getResult();

        for (Column<String> column : columns) {
            colMap.put(column.getName(), column.getStringValue());
        }
        return colMap;
    }

    /* (non-Javadoc)
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#writeTuple(backtype.storm.tuple.Tuple, backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuple(Tuple input, TupleMapper tupleMapper) throws Exception {
        String columnFamilyName = tupleMapper.mapToColumnFamily(input);
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        mutation.execute();
    }

    /* (non-Javadoc)
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#writeTuples(java.util.List, backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuples(List<Tuple> inputs, TupleMapper tupleMapper) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        for (Tuple input : inputs) {
            String columnFamilyName = tupleMapper.mapToColumnFamily(input);
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                    StringSerializer.get(), StringSerializer.get());
            this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        }
        mutation.execute();
    }

    private void addTupleToMutation(Tuple input, ColumnFamily<String, String> columnFamily, String rowKey,
            MutationBatch mutation, TupleMapper tupleMapper) {
        Map<String, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
    }

	@Override
	public void incrementCountColumn(Tuple input, TupleCounterMapper tupleMapper) throws Exception {
		String columnFamilyName = tupleMapper.mapToColumnFamily(input);
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        long incrementAmount = tupleMapper.mapToIncrementAmount(input);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
		StringSerializer.get(), StringSerializer.get());
        for(String columnName : tupleMapper.mapToColumnList(input)){
	mutation.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, incrementAmount);
        }
        mutation.execute();
	}

	@Override
	public void incrementCountColumns(List<Tuple> inputs,
			TupleCounterMapper tupleMapper) throws Exception {
		MutationBatch mutation = keyspace.prepareMutationBatch();
		for (Tuple input : inputs) {
			String columnFamilyName = tupleMapper.mapToColumnFamily(input);
	        String rowKey = (String) tupleMapper.mapToRowKey(input);
	        long incrementAmount = tupleMapper.mapToIncrementAmount(input);
	        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
	                StringSerializer.get(), StringSerializer.get());
	        for(String columnName : tupleMapper.mapToColumnList(input)){
		mutation.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, incrementAmount);
	        }
		}
		mutation.execute();

	}

}
