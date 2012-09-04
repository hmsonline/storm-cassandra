package backtype.storm.contrib.cassandra.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

public class CassandraBoltTest {
    private static Logger LOG = LoggerFactory.getLogger(CassandraBoltTest.class);

    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
            DataLoader dataLoader = new DataLoader("TestCluster", "localhost:9171");
            dataLoader.load(new ClassPathYamlDataSet("CassandraBoltTest.yaml"));
        } catch (Exception e) {
            logger.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }

    @AfterClass
    public static void teardownCassandra() {
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }
    

    @Test
    public void testBolt() throws Exception {
<<<<<<< HEAD
        EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
        CassandraBatchingBolt bolt = new CassandraBatchingBolt(EmbeddedCassandra.TEST_KS,
                EmbeddedCassandra.TEST_CF);
=======
        DefaultBatchingCassandraBolt bolt = new DefaultBatchingCassandraBolt("users",
                "VALUE");
>>>>>>> 206218588025f0eae7401e0f7e32b2fe926371a3
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        Fields fields = new Fields("VALUE");
        TopologyContext context = new MockTopologyContext(builder.createTopology(), fields);

        Config config = new Config();
<<<<<<< HEAD
        config.put(CassandraBolt.CASSANDRA_HOST, "localhost:9160");
        config.put(CassandraBolt.CASSANDRA_KEYSPACE, "test_ks");
=======
        config.put(CassandraConstants.CASSANDRA_HOST, "localhost:9171");
        config.put(CassandraConstants.CASSANDRA_KEYSPACE, "TestKeyspace");
>>>>>>> 206218588025f0eae7401e0f7e32b2fe926371a3
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);

        bolt.prepare(config, context, null);
        System.out.println("Bolt Preparation Complete.");

        Values values = new Values(42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);
        
        // wait very briefly for the batch to complete
        Thread.sleep(250);
        
        // TODO: verify the persistence

<<<<<<< HEAD
        Map<String, Map<String, String>> rows = embeddedCassandra.getRows();
        LOG.error("ROWS [" + rows.size() + "]");
        for (String row : rows.keySet()) {
            LOG.error("ROW [" + row + "]");
        }
=======
>>>>>>> 206218588025f0eae7401e0f7e32b2fe926371a3
    }
}
