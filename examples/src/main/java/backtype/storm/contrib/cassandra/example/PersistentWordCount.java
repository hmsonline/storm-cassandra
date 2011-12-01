package backtype.storm.contrib.cassandra.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;

public class PersistentWordCount {
	private static final String WORD_SPOUT = "WORD_SPOUT";
	private static final String COUNT_BOLT ="COUNT_BOLT";
	private static final String CASSANDRA_BOLT = "CASSANDRA_BOLT";
	
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
