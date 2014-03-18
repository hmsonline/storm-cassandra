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

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.AstyanaxClientFactory;
import com.hmsonline.storm.cassandra.exceptions.StormCassandraException;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

@Deprecated
public class TridentCassandraWriteFunction<K, C, V> implements Function {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraWriteFunction.class);
    protected TridentTupleMapper<K, C, V> tupleMapper;
    private AstyanaxClient<K, C, V> client;

    private String cassandraClusterId;
    private Object valueToEmit;
    
    public void setValueToEmitAfterWrite(Object valueToEmit) {
        this.valueToEmit = valueToEmit;
    }

    public TridentCassandraWriteFunction(String cassandraClusterId, TridentTupleMapper<K, C, V> tupleMapper) {
        this.tupleMapper = tupleMapper;
        this.cassandraClusterId = cassandraClusterId;
        this.valueToEmit = null;
    }
    public TridentCassandraWriteFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper,
            Object valueToEmit) {
        this(clientConfigKey, tupleMapper);
        this.valueToEmit = valueToEmit;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void prepare(Map stormConf, TridentOperationContext context) {
        Map<String, Object> config = (Map<String, Object>) stormConf.get(this.cassandraClusterId);
        client = AstyanaxClientFactory.getInstance(cassandraClusterId, config);
    }

    @Override
    public void cleanup() {
        this.client.stop();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            writeTuple(tuple);
            if (this.valueToEmit != null) {
                collector.emit(new Values(this.valueToEmit));
            }
        } catch (TupleMappingException e) {
            LOG.error("Skipping tuple: " + tuple, e);
        } catch (StormCassandraException e) {
            LOG.error("Failed to write tuple. Exception: " + e.getLocalizedMessage());
            // This will tell storm to replay the whole batch
            // TODO should we add a number of retry here?
            throw new FailedException();
        } catch (Exception e) {
            LOG.error("Unexcepted exception: " + e.getLocalizedMessage());
            // unexpected error should not be replayed. Log only
            collector.reportError(e);
        }
    }

    public void writeTuple(TridentTuple input) throws Exception {
        this.client.writeTuple(input, this.tupleMapper);
    }
}
