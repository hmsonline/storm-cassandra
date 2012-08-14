package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

public class CassandraBoltTest {
    private static Logger logger = LoggerFactory.getLogger(CassandraBoltTest.class);

    @Test
    public void testBolt() throws Exception {
        EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
        DefaultBatchingCassandraBolt bolt = new DefaultBatchingCassandraBolt(EmbeddedCassandra.TEST_KS,
                EmbeddedCassandra.TEST_CF);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        TopologyContext context = new MockTopologyContext(builder.createTopology());

        Config config = new Config();
        config.put(CassandraConstants.CASSANDRA_HOST, "localhost");
        config.put(CassandraConstants.CASSANDRA_KEYSPACE, "hms_data");
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);

        bolt.prepare(config, context, null);

        List<Object> values = new ArrayList<Object>();
        values.add(42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        Map<String, Map<String, String>> rows = embeddedCassandra.getRows();
        System.err.println("ROWS [" + rows.size() + "]");
        for (String row : rows.keySet()) {
            System.err.println("ROW [" + row + "]");
        }
    }

}
