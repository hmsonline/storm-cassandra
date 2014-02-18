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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 * This is a batching bolt that can be used outside of a transactional topology.
 * It does *not* implement IBatchBolt for that reason. If you want to use the
 * batching inside a transactional topology, use
 * <code>TransactionalBatchingCassandraBolt</code>.
 *
 */
@SuppressWarnings("serial")
public class CassandraBatchingBolt<K, C, V> extends AbstractBatchingBolt<K, C, V> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBatchingBolt.class);

    public CassandraBatchingBolt(String clientConfigKey, TupleMapper<K, C, V> tupleMapper) {
        super(clientConfigKey, tupleMapper);
    }

    @Override
    public void executeBatch(List<Tuple> inputs) {
        try {
            this.writeTuples(inputs, tupleMapper);
            // NOTE: Changed this to ack on all or none since that is how the
            // mutation executes.
            if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
                for (Tuple tupleToAck : inputs) {
                    this.collector.ack(tupleToAck);
                }
            }
        } catch (Throwable e) {
            LOG.error("Unable to write batch.", e);
            for (Tuple tupleToAck : inputs) {
                this.collector.fail(tupleToAck);
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // By default we don't emit anything.
    }

}
