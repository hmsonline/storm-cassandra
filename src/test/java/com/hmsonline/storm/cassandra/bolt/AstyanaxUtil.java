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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxUtil.class);
    
    public static Builder newBuilder(String hostString){
        return new AstyanaxContext.Builder()
        .forCluster("ClusterName")
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
        .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1)
                        .setSeeds(hostString))
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
    }


    public static AstyanaxContext<Keyspace> newContext(String hostString, String keyspace) {
        Builder builder = newBuilder(hostString);
        AstyanaxContext<Keyspace> context = builder.forKeyspace(keyspace).buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }
    
    public static AstyanaxContext<Cluster> newClusterContext(String hostString){
        Builder builder = newBuilder(hostString);
        AstyanaxContext<Cluster> context = builder.buildCluster(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }
    
    public static void createColumnFamily(AstyanaxContext<Cluster> ctx,String ks, String cf, String comparator, 
    		String keyValidator, String defValidator) throws ConnectionException{
    	createColumnFamily(ctx, ks, cf, comparator, keyValidator, defValidator, false);
    }
    
    public static void createColumnFamily(AstyanaxContext<Cluster> ctx,String ks, String cf, String comparator, 
    		String keyValidator, String defValidator, boolean dropFirst) throws ConnectionException{
        Cluster cluster = ctx.getEntity();
        KeyspaceDefinition keyspace = cluster.describeKeyspace(ks);
        if(keyspace != null){
            LOG.warn("Keyspace {} already exists.", ks);
        } else{
            LOG.warn("Creating keyspace '{}'", ks);
            KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();
            Map<String, String> stratOptions = new HashMap<String, String>();
            stratOptions.put("replication_factor", "1");
            ksDef.setName(ks)
                    .setStrategyClass("SimpleStrategy")
                    .setStrategyOptions(stratOptions);
            cluster.addKeyspace(ksDef);
        }
        
        if (dropFirst){
            LOG.warn("Dropping {} column family.", cf);
            try{
        	cluster.dropColumnFamily(ks,cf);
            } catch (BadRequestException bre){
            	LOG.warn("Could not drop column family, likely doesn't exist. [" + bre.getMessage() + "]");
            }
        }
        
        LOG.warn("Adding column family: '{}'", cf);
        try {
        cluster.addColumnFamily(cluster.makeColumnFamilyDefinition().setKeyspace(ks).setName(cf).setComparatorType(comparator)
                .setKeyValidationClass(keyValidator).setDefaultValidationClass(defValidator));
        } catch (BadRequestException bre){
        	LOG.warn("Could not create column family [" + bre.getMessage() + "]");
        }
    }
    
    public static void createCounterColumnFamily(AstyanaxContext<Cluster> ctx,String ks, String cf) throws ConnectionException{
        Cluster cluster = ctx.getEntity();
        Keyspace keyspace = cluster.getKeyspace(ks);
        if(keyspace != null){
            LOG.warn("Keyspace {} already exists.", ks);
        }
        KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();

        Map<String, String> stratOptions = new HashMap<String, String>();
        stratOptions.put("replication_factor", "1");
        ksDef.setName(ks)
                .setStrategyClass("SimpleStrategy")
                .setStrategyOptions(stratOptions)
                .addColumnFamily(
                        cluster.makeColumnFamilyDefinition().setName(cf).setComparatorType("UTF8Type")
                                .setKeyValidationClass("UTF8Type").setDefaultValidationClass("CounterColumnType"));

        cluster.addKeyspace(ksDef);
    }

}
