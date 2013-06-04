package com.hmsonline.storm.cassandra.bolt;

import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.createColumnFamily;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newClusterContext;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.OpaqueValue;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.trident.CassandraMapState;
import com.hmsonline.storm.cassandra.trident.CassandraUpdater;
import com.hmsonline.storm.cassandra.trident.CassandraMapState.Options;
import com.hmsonline.storm.cassandra.trident.CassandraStateFactory;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;

public class CassandraStateTest {
    private static Logger LOG = LoggerFactory.getLogger(CassandraStateTest.class);
    private static String KEYSPACE = CassandraStateTest.class.getSimpleName().toLowerCase();
    


    
    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
//        SingletonEmbeddedCassandra.getInstance();
        try {

            AstyanaxContext<Cluster> clusterContext = newClusterContext("localhost:9160");

            createColumnFamily(clusterContext, KEYSPACE, "trident", "UTF8Type", "UTF8Type", "UTF8Type");

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }
    


    
    
    @Test
    public void testCassandraMapState() throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"), 300, 
                new Values("the cow jumped over the moon"), 
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), 
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {KEYSPACE}));
        Config config = new Config();
        config.setMaxSpoutPending(25);
        config.put("cassandra.config", clientConfig);
        
        TridentTupleMapper tm = new SimpleTridentTupleMapper(KEYSPACE, new Fields("word"));

        StateFactory cassandraStateFactory = new CassandraStateFactory("cassandra.config", tm);

        topology.newStream("spout1", spout)
        .parallelismHint(3)
        .shuffle()
        .each(new Fields("sentence"), new Split(), new Fields("word"))
        .each(new Fields("word"), new LogAndEmitFunction(), new Fields("key1"))
//        .each(new Fields("word", "key1"), new LogAndEmitFunction(), new Fields("key2"))
        .partitionPersist(cassandraStateFactory, new Fields("word", "key1"), new CassandraUpdater(),new Fields("word", "key1"))
        .newValuesStream()
        .each(new Fields("word", "key1"), new LogFunction(), new Fields());

//        TridentState wordCounts = topology.newStream("spout1", spout)
//                .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
//                .partitionAggregate(inputFields, agg, functionFields)(cassandraStateFactory, new Count(), new Fields("count")).parallelismHint(1);

//        LocalDRPC client = new LocalDRPC();
//        topology.newDRPCStream("words", client).each(new Fields("args"), new Split(), new Fields("word"))
//                .groupBy(new Fields("word"))
//                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
//                .each(new Fields("count"), new FilterNull())
//                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology.build());

            Thread.sleep(30000);

            


        cluster.shutdown();
    }
}
