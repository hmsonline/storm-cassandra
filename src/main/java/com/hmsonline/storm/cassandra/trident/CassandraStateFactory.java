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
package com.hmsonline.storm.cassandra.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;
import backtype.storm.utils.Utils;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.AstyanaxClientFactory;
import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;

public class CassandraStateFactory implements StateFactory {

    private static final long serialVersionUID = 1055824326488179872L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraStateFactory.class);

    private String cassandraClusterId;
    private ExceptionHandler exceptionHandler;

    /**
     * @param  Identifier that uniquely identifies the Cassandra Cluster
     */
    public CassandraStateFactory(String cassandraClusterId) {
        this(cassandraClusterId, null);
    }

    /**
     * @param cassandraClusterId Identifier that uniquely identifies the Cassandra Cluster
     * @param exceptionHandler
     */
    public CassandraStateFactory(String cassandraClusterId, ExceptionHandler exceptionHandler) {
        this.cassandraClusterId = cassandraClusterId;
        this.exceptionHandler = exceptionHandler;
    }

    /* (non-Javadoc)
     * @see storm.trident.state.StateFactory#makeState(java.util.Map, backtype.storm.task.IMetricsContext, int, int)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("Making new CassandraState object for cluster " + cassandraClusterId + ": partition [" + partitionIndex + "] of [" + numPartitions + "]");
        AstyanaxClient client = AstyanaxClientFactory.getInstance(cassandraClusterId, (Map)conf.get(cassandraClusterId));
        int batchMaxSize = Utils.getInt(Utils.get(conf, StormCassandraConstants.CASSANDRA_BATCH_MAX_SIZE,
                CassandraState.DEFAULT_MAX_BATCH_SIZE));
        return new CassandraState(client, batchMaxSize, this.exceptionHandler);
    }

}
