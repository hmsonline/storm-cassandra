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
package com.hmsonline.storm.cassandra.bolt;

import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.createColumnFamily;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newClusterContext;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newContext;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
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

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraBoltTest {
    private static Logger LOG = LoggerFactory.getLogger(CassandraBoltTest.class);
    private static String KEYSPACE = CassandraBoltTest.class.getSimpleName().toLowerCase();


    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        SingletonEmbeddedCassandra.getInstance();
        try {

            AstyanaxContext<Cluster> clusterContext = newClusterContext("localhost:9160");
            createColumnFamily(clusterContext, KEYSPACE, "users","UTF8Type", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "Counts", "UTF8Type", "UTF8Type", "CounterColumnType", true);

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }


    @Test
    public void testBolt() throws Exception {
        TupleMapper<String, String, String> tupleMapper = new DefaultTupleMapper(KEYSPACE, "users", "VALUE");
        String configKey = "cassandra-config";
        CassandraBatchingBolt<String, String, String> bolt = new CassandraBatchingBolt<String, String, String>(configKey, tupleMapper);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        Fields fields = new Fields("VALUE");
        TopologyContext context = new MockTopologyContext(builder.createTopology(), fields);

        Config config = new Config();
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);
        
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {KEYSPACE}));
        config.put(configKey, clientConfig);

        bolt.prepare(config, context, null);
        System.out.println("Bolt Preparation Complete.");

        Values values = new Values(42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        // wait very briefly for the batch to complete
        Thread.sleep(250);

        AstyanaxContext<Keyspace> astyContext = newContext("localhost:9160", KEYSPACE);
        Keyspace ks = astyContext.getEntity();

        Column<String> result = ks
                .prepareQuery(new ColumnFamily<String, String>("users", StringSerializer.get(), StringSerializer.get()))
                .getKey("42").getColumn("VALUE").execute().getResult();
        assertEquals("42", result.getStringValue());

    }

    @Test
    public void testCounterBolt() throws Exception {
        String configKey = "cassandra-config";
        CassandraCounterBatchingBolt<String, String,Long> bolt = new CassandraCounterBatchingBolt<String, String, Long>(KEYSPACE, configKey, "Counts", "Timestamp", "IncrementAmount");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST__COUNTER_BOLT", bolt);

        Fields fields = new Fields("Timestamp", "IncrementAmount", "CounterColumn");
        TopologyContext context = new MockTopologyContext(builder.createTopology(), fields);

        Config config = new Config();
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);
        
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {KEYSPACE}));
        config.put(configKey, clientConfig);
        

        bolt.prepare(config, context, null);
        System.out.println("Bolt Preparation Complete.");

        Values values = new Values("1", 1L, "MyCountColumn");
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        // wait very briefly for the batch to complete
        Thread.sleep(250);

        AstyanaxContext<Keyspace> astyContext = newContext("localhost:9160", KEYSPACE);
        Keyspace ks = astyContext.getEntity();

        Column<String> result = ks
                .prepareQuery(
                        new ColumnFamily<String, String>("Counts", StringSerializer.get(), StringSerializer.get()))
                .getKey("1").getColumn("MyCountColumn").execute().getResult();
        assertEquals(1L, result.getLongValue());
    }
}
