/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hmsonline.storm.cassandra.example;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
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

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.CassandraLookupBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.ColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.ValuelessColumnsMapper;

@SuppressWarnings("deprecation")
public class CassandraReachTopology {

    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");

        Config config = new Config();
        String configKey = "cassandra-config";
        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {"stormks"}));
        config.put(configKey, clientConfig);

        // DelimitedColumnLookupBolt tweetersBolt =
        // new DelimitedColumnLookupBolt("tweeters_delimited", "rowKey",
        // "tweeted_by", ":", "rowKey", "tweeter", true);
        //
        // DelimitedColumnLookupBolt followersBolt =
        // new DelimitedColumnLookupBolt("followers_delimited", "tweeter",
        // "followers", ":", "rowKey", "follower", true);

        // cf = "tweeters", rowkey = tuple["url"]
        TupleMapper<String, String, String> tweetersTupleMapper = new DefaultTupleMapper("stormks", "tweeters", "url");
        // cf (url -> tweeters) -> emit(url, follower)
        ColumnMapper<String, String, String> tweetersColumnsMapper = new ValuelessColumnsMapper("url", "tweeter", true);
        CassandraLookupBolt<String, String, String> tweetersBolt = new CassandraLookupBolt<String, String, String>(configKey,
                tweetersTupleMapper, tweetersColumnsMapper);

        // cf = "followers", rowkey = tuple["tweeter"]
        TupleMapper<String, String, String> followersTupleMapper = new DefaultTupleMapper("stormks", "followers", "tweeter");
        // cf (tweeter -> followers) ==> emit(url, follower)
        ValuelessColumnsMapper followersColumnsMapper = new ValuelessColumnsMapper("url", "follower", true);
        CassandraLookupBolt<String, String, String> followersBolt = new CassandraLookupBolt<String, String, String>(configKey,
                followersTupleMapper, followersColumnsMapper);

        builder.addBolt(new InitBolt());
        builder.addBolt(tweetersBolt).shuffleGrouping();
        builder.addBolt(followersBolt).shuffleGrouping();
        builder.addBolt(new PartialUniquer()).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator()).fieldsGrouping(new Fields("id"));

        if (args == null || args.length == 0) {
            config.setMaxTaskParallelism(3);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            if ("true".equals(System.getProperty("debug"))) {
                config.setDebug(true);
            }
            cluster.submitTopology("reach-drpc", config, builder.createLocalTopology(drpc));

            String[] urlsToTry = new String[] { "http://github.com/hmsonline", "http://github.com/nathanmarz",
                    "http://github.com/ptgoetz", "http://github.com/boneill" };
            for (String url : urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {
            config.setNumWorkers(6);
            StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());
        }
    }

    @SuppressWarnings("serial")
    public static class InitBolt implements IBasicBolt {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "url"));
        }

        @SuppressWarnings("rawtypes")
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

    @SuppressWarnings({ "serial", "rawtypes" })
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

    @SuppressWarnings({ "serial", "rawtypes" })
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
