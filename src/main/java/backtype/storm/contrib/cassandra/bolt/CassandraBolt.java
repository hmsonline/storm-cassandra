package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.task.TopologyContext;
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

@SuppressWarnings("serial")
public abstract class CassandraBolt implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);
    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    
    private String cassandraHost;
    private String cassandraKeyspace;
    protected Cluster cluster;
    protected Keyspace keyspace;
    protected TupleMapper tupleMapper;
    protected AstyanaxContext<Keyspace> astyanaxContext;

    public CassandraBolt(TupleMapper tupleMapper) {
        this.tupleMapper = tupleMapper;
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        initCassandraConnection();
    }

    private void initCassandraConnection() {
        try {
            this.astyanaxContext = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(this.cassandraKeyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                    .withConnectionPoolConfiguration(
                            new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1).setSeeds(
                                    this.cassandraHost)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            this.astyanaxContext.start();
            this.keyspace = this.astyanaxContext.getEntity();
            // test the connection
            this.keyspace.describeKeyspace();
        } catch (Throwable e) {
            LOG.warn("Preparation failed.", e);
            throw new IllegalStateException("Failed to prepare CassandraBolt", e);
        }
    }    

    public void cleanup(){
        this.astyanaxContext.shutdown();
    }

    public void writeTuple(Tuple input) throws ConnectionException {
        String columnFamilyName = tupleMapper.mapToColumnFamily(input);
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        this.addTupleToMutation(input, columnFamily, rowKey, mutation);
        mutation.execute();
    }

    public void writeTuples(List<Tuple> inputs) throws ConnectionException {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        for (Tuple input : inputs) {
            String columnFamilyName = tupleMapper.mapToColumnFamily(input);
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                    StringSerializer.get(), StringSerializer.get());
            this.addTupleToMutation(input, columnFamily, rowKey, mutation);
        }
        mutation.execute();
    }

    private void addTupleToMutation(Tuple input, ColumnFamily<String, String> columnFamily, String rowKey,
            MutationBatch mutation) {
        Map<String, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
