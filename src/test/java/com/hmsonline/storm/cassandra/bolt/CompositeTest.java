package com.hmsonline.storm.cassandra.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CompositeTest {
    private static Logger LOG = LoggerFactory.getLogger(CompositeTest.class);
    private static String KEYSPACE = CompositeTest.class.getSimpleName();

    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        SingletonEmbeddedCassandra.getInstance();
        try {
            
            AstyanaxContext<Cluster> clusterContext = newClusterContext("localhost:9160");

            createColumnFamily(clusterContext, KEYSPACE, "simple");

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }

    


    @Test
    public void testCompositeKey() throws Exception {
        TupleMapper tupleMapper = new SimpleTupleMapper("simple");
        AstyanaxClient client = new AstyanaxClient();
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, KEYSPACE);
        client.start(clientConfig);
        
        Fields fields = new Fields("key1", "key2", "foo", "bar");
        
        TopologyBuilder builder = new TopologyBuilder();
        MockTopologyContext ctx = new MockTopologyContext(builder.createTopology(), fields);

        Values values = new Values("key1val", "key2val", "fooval", "barval");
        Tuple tuple = new TupleImpl(ctx, values, 0, "test");
        
        client.writeTuple(tuple, tupleMapper);
        Thread.sleep(2000);
        
//        AstyanaxContext<Keyspace> astyContext

/*

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
*/
    }
    
    
    public static Builder newBuilder(String hostString){
        return new AstyanaxContext.Builder()
        .forCluster("ClusterName")
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
        .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1)
                        .setSeeds(hostString))
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
    }


    public static AstyanaxContext<Keyspace> newContext(String hostString, String keyspace) {
        Builder builder = newBuilder(hostString);
        AstyanaxContext<Keyspace> context = builder.forKeyspace(keyspace).buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }
    
    public static AstyanaxContext<Cluster> newClusterContext(String hostString){
        Builder builder = newBuilder(hostString);
        AstyanaxContext<Cluster> context = builder.buildCluster(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }
    
    
    public static void createColumnFamily(AstyanaxContext<Cluster> ctx,String ks, String cf) throws ConnectionException{
        Cluster cluster = ctx.getEntity();
        KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();

        Map<String, String> stratOptions = new HashMap<String, String>();
        stratOptions.put("replication_factor", "1");
        ksDef.setName(ks)
                .setStrategyClass("SimpleStrategy")
                .setStrategyOptions(stratOptions)
                .addColumnFamily(
                        cluster.makeColumnFamilyDefinition().setName(cf).setComparatorType("UTF8Type")
                                .setKeyValidationClass("UTF8Type").setDefaultValidationClass("UTF8Type"));

        cluster.addKeyspace(ksDef);
    }
}
