package com.hmsonline.storm.cassandra.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;

public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String CASSANDRA_BOLT = "WORD_COUNT_CASSANDRA_BOLT";

    public static void main(String[] args) throws Exception {
        Config config = new Config();

        config.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        config.put(StormCassandraConstants.CASSANDRA_KEYSPACE, "stormks");

        TestWordSpout wordSpout = new TestWordSpout();

        TestWordCounter countBolt = new TestWordCounter();

        // create a CassandraBolt that writes to the "stormcf" column
        // family and uses the Tuple field "word" as the row key
        CassandraBatchingBolt<String, String> cassandraBolt = new CassandraBatchingBolt<String, String>(
                new DefaultTupleMapper("stormcf", "word"), String.class, String.class);
        cassandraBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);

        // setup topology:
        // wordSpout ==> countBolt ==> cassandraBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, wordSpout, 3);
        builder.setBolt(COUNT_BOLT, countBolt, 3).fieldsGrouping(WORD_SPOUT, new Fields("word"));
        builder.setBolt(CASSANDRA_BOLT, cassandraBolt, 3).shuffleGrouping(COUNT_BOLT);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
