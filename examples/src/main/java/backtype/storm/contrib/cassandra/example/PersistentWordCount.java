package backtype.storm.contrib.cassandra.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;

public class PersistentWordCount {
	private static final int WORD_SPOUT = 1;
	private static final int COUNT_BOLT = 2;
	private static final int CASSANDRA_BOLT = 3;
	
	public static void main(String[] args) throws Exception{
		Config config = new Config();
		
		config.put(CassandraBolt.CASSANDRA_HOST, "localhost");
		config.put(CassandraBolt.CASSANDRA_PORT, 9160);
		config.put(CassandraBolt.CASSANDRA_KEYSPACE, "stormks");
		
		TestWordSpout wordSpout = new TestWordSpout();
		
		TestWordCounter countBolt = new TestWordCounter();
		
		// create a CassandraBolt that writes to the "stormcf" column
		// family and uses the Tuple field "word" as the row key
		IRichBolt cassandraBolt = new CassandraBolt("stormcf", "word");
		
		// setup topology:
		// wordSpout ==> countBolt ==> cassandraBolt
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(WORD_SPOUT, wordSpout);
		builder.setBolt(COUNT_BOLT, countBolt).shuffleGrouping(WORD_SPOUT);
		builder.setBolt(CASSANDRA_BOLT, cassandraBolt).shuffleGrouping(COUNT_BOLT);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
