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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.OpaqueValue;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.trident.CassandraMapState;
import com.hmsonline.storm.cassandra.trident.CassandraMapState.Options;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;

public class CassandraMapStateTest {
    private static Logger LOG = LoggerFactory.getLogger(CassandraMapStateTest.class);
    private static String KEYSPACE = CassandraMapStateTest.class.getSimpleName().toLowerCase();
    
    private enum TransactionType {
        TRANSACTIONAL, NON_TRANSACTIONAL, OPAQUE;
    }

    
    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        SingletonEmbeddedCassandra.getInstance();
        try {

            AstyanaxContext<Cluster> clusterContext = newClusterContext("localhost:9160");

            createColumnFamily(clusterContext, KEYSPACE, "transactional", "UTF8Type", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "nontransactional", "UTF8Type", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "opaque", "UTF8Type", "UTF8Type", "UTF8Type");

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }
    
    @Test
    public void testOpaqueTransactionalState() throws Exception {
        testCassandraMapState(TransactionType.OPAQUE);
    }

    @Test
    public void testTransactionalState() throws Exception {
        testCassandraMapState(TransactionType.TRANSACTIONAL);
    }
    
    @Test
    public void testNonTransactionalState() throws Exception {
        testCassandraMapState(TransactionType.NON_TRANSACTIONAL);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void testCassandraMapState(TransactionType txType) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"), 3, 
                new Values("the cow jumped over the moon"), 
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), 
                new Values("how many apples can you eat"));
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();

        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_STATE_KEYSPACE, KEYSPACE);
        Config config = new Config();
        config.setMaxSpoutPending(25);
        config.put("cassandra.config", clientConfig);

        StateFactory cassandraStateFactory = null;
        Options options = null;
        switch(txType){
        case TRANSACTIONAL:
            options = new Options<TransactionalValue>();
            options.columnFamily = "transactional";
            cassandraStateFactory = CassandraMapState.transactional(options);            
            break;
            
        case OPAQUE:
            options = new Options<OpaqueValue>();
            options.columnFamily = "opaque";
            cassandraStateFactory = CassandraMapState.opaque(options);
            break;
            
        case NON_TRANSACTIONAL:
            options = new Options<Object>();
            options.columnFamily = "nontransactional";
            cassandraStateFactory = CassandraMapState.nonTransactional(options);
            break;
        }
        

        TridentState wordCounts = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
                .persistentAggregate(cassandraStateFactory, new Count(), new Fields("count")).parallelismHint(1);

        LocalDRPC client = new LocalDRPC();
        topology.newDRPCStream("words", client).each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology.build());

            Thread.sleep(10000); 


            assertEquals("[[5]]", client.execute("words", "cat dog the man")); // 5
            assertEquals("[[0]]", client.execute("words", "cat")); // 0
            assertEquals("[[0]]", client.execute("words", "dog")); // 0
            assertEquals("[[4]]", client.execute("words", "the")); // 4
            assertEquals("[[1]]", client.execute("words", "man")); // 1
            


        cluster.shutdown();
        client.shutdown();
    }
}
