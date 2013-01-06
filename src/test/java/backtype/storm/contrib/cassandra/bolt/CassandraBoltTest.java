package backtype.storm.contrib.cassandra.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

public class CassandraBoltTest {
    private static Logger LOG = LoggerFactory.getLogger(CassandraBoltTest.class);
    private static EmbeddedCassandra cassandra;


    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        try {
            cassandra = new EmbeddedCassandra(9171);
            cassandra.start();
            Thread.sleep(2000);

            AstyanaxContext<Cluster> clusterContext = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                    .withConnectionPoolConfiguration(
                            new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1).setSeeds(
                                    "localhost:9171")).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildCluster(ThriftFamilyFactory.getInstance());

            clusterContext.start();
            Cluster cluster = clusterContext.getEntity();
            KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();

            Map<String, String> stratOptions = new HashMap<String, String>();
            stratOptions.put("replication_factor", "1");
            ksDef.setName("TestKeyspace")
                    .setStrategyClass("SimpleStrategy")
                    .setStrategyOptions(stratOptions)
                    .addColumnFamily(
                            cluster.makeColumnFamilyDefinition().setName("users").setComparatorType("UTF8Type")
                                    .setKeyValidationClass("UTF8Type").setDefaultValidationClass("UTF8Type"))
                    .addColumnFamily(cluster.makeColumnFamilyDefinition().setName("Counts").setComparatorType("UTF8Type")
                                    .setKeyValidationClass("UTF8Type").setDefaultValidationClass("CounterColumnType"));

            cluster.addKeyspace(ksDef);
            Thread.sleep(2000);

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }

    @AfterClass
    public static void teardownCassandra() {
        cassandra.stop();
    }

    @Test
    public void testBolt() throws Exception {
        CassandraBatchingBolt bolt = new CassandraBatchingBolt("users", "VALUE");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        Fields fields = new Fields("VALUE");
        TopologyContext context = new MockTopologyContext(builder.createTopology(), fields);

        Config config = new Config();
        config.put(CassandraBolt.CASSANDRA_HOST, "localhost:9171");
        config.put(CassandraBolt.CASSANDRA_KEYSPACE, "TestKeyspace");
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);

        bolt.prepare(config, context, null);
        System.out.println("Bolt Preparation Complete.");

        Values values = new Values(42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        // wait very briefly for the batch to complete
        Thread.sleep(250);

        AstyanaxContext<Keyspace> astyContext = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace("TestKeyspace")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1).setSeeds(
                                "localhost:9171")).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        astyContext.start();
        Keyspace ks = astyContext.getEntity();

        Column<String> result = ks
                .prepareQuery(new ColumnFamily<String, String>("users", StringSerializer.get(), StringSerializer.get()))
                .getKey("42").getColumn("VALUE").execute().getResult();
        assertEquals("42", result.getStringValue());

    }
    
    @Test
    public void testCounterBolt() throws Exception {
    	CassandraCounterBatchingBolt bolt = new CassandraCounterBatchingBolt("Counts", "Timestamp", "IncrementAmount");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST__COUNTER_BOLT", bolt);

        Fields fields = new Fields("Timestamp","IncrementAmount","CounterColumn");
        TopologyContext context = new MockTopologyContext(builder.createTopology(), fields);

        Config config = new Config();
        config.put(CassandraBolt.CASSANDRA_HOST, "localhost:9171");
        config.put(CassandraBolt.CASSANDRA_KEYSPACE, "TestKeyspace");
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);

        bolt.prepare(config, context, null);
        System.out.println("Bolt Preparation Complete.");

        Values values = new Values(1L,1L,"MyCountColumn");
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        // wait very briefly for the batch to complete
        Thread.sleep(250);

        AstyanaxContext<Keyspace> astyContext = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace("TestKeyspace")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1).setSeeds(
                                "localhost:9171")).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        astyContext.start();
        Keyspace ks = astyContext.getEntity();
        
        Column<String> result = ks
                .prepareQuery(new ColumnFamily<String, String>("Counts", StringSerializer.get(), StringSerializer.get()))
                .getKey("1").getColumn("MyCountColumn").execute().getResult();
        assertEquals(1L, result.getLongValue());

    }
}
