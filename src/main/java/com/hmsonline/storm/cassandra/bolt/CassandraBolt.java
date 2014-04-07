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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;

@SuppressWarnings("serial")
public abstract class CassandraBolt<K, C, V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);

    private String clientConfigKey;

    protected transient AstyanaxClient<K, C, V> client;

    protected TupleMapper<K, C, V> tupleMapper;

    public CassandraBolt(String clientConfigKey, TupleMapper<K, C, V> tupleMapper) {
        this.tupleMapper = tupleMapper;
        this.clientConfigKey = clientConfigKey;

        LOG.debug("Creating Cassandra Bolt (" + this + ")");
    }

    @SuppressWarnings("unchecked")
    public void prepare(Map<String, Object> stormConf, TopologyContext context) {
        Map<String, Object> config = (Map<String, Object>) stormConf.get(this.clientConfigKey);
        this.client = new AstyanaxClient<K, C, V>();
        this.client.start(config);
    }

    public void cleanup() {
        this.client.stop();
    }

    public void writeTuple(Tuple input, TupleMapper<K, C, V> tupleMapper) throws Exception {
        this.client.writeTuple(input, tupleMapper);
    }

    public void writeTuples(List<Tuple> inputs, TupleMapper<K, C, V> tupleMapper) throws Exception {
        this.client.writeTuples(inputs, tupleMapper);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void incrementCounter(Tuple input, TupleCounterMapper tupleMapper) throws Exception {
        this.client.incrementCountColumn(input, tupleMapper);
    }

    public void incrementCounters(List<Tuple> inputs, TupleCounterMapper tupleMapper) throws Exception {
        this.client.incrementCountColumns(inputs, tupleMapper);
    }
}
