// Copyright (c) 2012 P. Taylor Goetz

package backtype.storm.contrib.cassandra.example;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.cassandra.bolt.CassandraConstants;
import backtype.storm.contrib.cassandra.bolt.ValueLessColumnLookupBolt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CassandraReachTopology implements CassandraConstants{

    public static void main(String[] args) throws Exception{
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        
//        DelimitedColumnLookupBolt tweetersBolt = 
//                        new DelimitedColumnLookupBolt("tweeters_delimited", "rowKey", "tweeted_by", ":", "rowKey", "tweeter", true);
//        
//        DelimitedColumnLookupBolt followersBolt = 
//                        new DelimitedColumnLookupBolt("followers_delimited", "tweeter", "followers", ":", "rowKey", "follower", true);
        
        ValueLessColumnLookupBolt tweetersBolt = 
                        new ValueLessColumnLookupBolt("tweeters", "rowKey","rowKey", "tweeter", true);
        
        ValueLessColumnLookupBolt followersBolt = 
                        new ValueLessColumnLookupBolt("followers", "tweeter", "rowKey", "follower", true);
        
        builder.addBolt(new InitBolt());
        builder.addBolt(tweetersBolt).shuffleGrouping();
        builder.addBolt(followersBolt).shuffleGrouping();
        builder.addBolt(new PartialUniquer()).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator()).fieldsGrouping(new Fields("id"));
        
        
        Config config = new Config();
        config.put(CASSANDRA_HOST, "localhost:9160");
        config.put(CASSANDRA_KEYSPACE, "stormks");
        
        if(args==null || args.length==0) {
            config.setMaxTaskParallelism(3);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            if("true".equals(System.getProperty("debug"))){
            	config.setDebug(true);
            }
            cluster.submitTopology("reach-drpc", config, builder.createLocalTopology(drpc));
            
            String[] urlsToTry = new String[] {"http://github.com/hmsonline","http://github.com/nathanmarz", "http://github.com/ptgoetz", "http://github.com/boneill"};
            for(String url: urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
            }
            
            cluster.shutdown();
            drpc.shutdown();
        } else {
            config.setNumWorkers(6);
            StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());
        }
    }
    
    public static class InitBolt implements IBasicBolt {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "rowKey")); 
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            collector.emit(input.getValues());
        }

        @Override
        public void cleanup() {

        }

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
        
    }
    
    @SuppressWarnings("serial")
	public static class PartialUniquer extends BaseBatchBolt {
        BatchOutputCollector collector;
        private Object id;
        Set<String> set = new HashSet<String>();
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            this.collector = collector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            this.set.add(tuple.getString(1));
        }


        @Override
        public void finishBatch() {
        	collector.emit(new Values(this.id, this.set.size()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }

    }
    
    public static class CountAggregator extends BaseBatchBolt {
        Object id;
        BatchOutputCollector collector;
        int count = 0;
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            this.collector = collector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
        	this.count += tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            this.collector.emit(new Values(this.id, this.count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }

        
    }
    
}
