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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 *
 * @param <K>
 * @param <C>
 * @param <V>
 */
@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
public class TridentCassandraBatchBolt<K, C, V> extends TransactionalCassandraBatchBolt implements ITridentBatchBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraBatchBolt.class);
    private Object transactionId = null;

    public TridentCassandraBatchBolt(String clientConfigKey, TupleMapper<K, C, V> tupleMapper) {
        super(clientConfigKey, tupleMapper);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector) {
        super.prepare(conf, context, collector, "TridentBolt");
    }

    @Override
    public void finishBatch() {
        List<Tuple> batch = new ArrayList<Tuple>();
        int size = queue.drainTo(batch);
        LOG.debug("Finishing batch for [" + transactionId + "], writing [" + size + "] tuples.");
        try {
            this.writeTuples(batch, this.tupleMapper);
        } catch (Exception e) {
            LOG.error("Could not write batch to cassandra.", e);
        }
    }

    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {
        super.execute(tuple);
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {
        super.finishBatch();
    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        // TODO Auto-generated method stub
        return null;
    }

}
