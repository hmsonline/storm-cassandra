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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.ColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.Equality;
import com.hmsonline.storm.cassandra.bolt.mapper.RangeQueryTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 * A bolt implementation that emits tuples based on a combination of cassandra
 * rowkey, columnkey, and delimiter.
 * <p/>
 * 
 * @author tgoetz
 */
@SuppressWarnings("serial")
public class CassandraLookupBolt<K, C, V> extends CassandraBolt<K, C, V> implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraLookupBolt.class);
    private ColumnMapper<K, C, V> columnsMapper;
    private RangeQueryTupleMapper<K, C, V> queryTupleMapper = null;

    public CassandraLookupBolt(String clientConfigKey, TupleMapper<K, C, V> tupleMapper,
            ColumnMapper<K, C, V> columnsMapper) {
        super(clientConfigKey, tupleMapper);
        this.columnsMapper = columnsMapper;
    }

    public CassandraLookupBolt(String clientConfigKey, RangeQueryTupleMapper<K, C, V> queryMapper,
            ColumnMapper<K, C, V> columnsMapper) {
        super(clientConfigKey, queryMapper);
        this.queryTupleMapper = queryMapper;
        this.columnsMapper = columnsMapper;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        String columnFamily = tupleMapper.mapToColumnFamily(input);
        K rowKey = tupleMapper.mapToRowKey(input);
        try {
            Map<C, V> colMap = null;
            if (queryTupleMapper != null) {
                C start = queryTupleMapper.mapToStartkey(input);
                C end = queryTupleMapper.mapToEndkey(input);
                colMap = this.client.lookup(this.queryTupleMapper, input, start, end, Equality.EQUAL);
            } else {
                colMap = this.client.lookup(this.tupleMapper, input);
            }

            List<Values> valuesToEmit = columnsMapper.mapToValues(rowKey, colMap, input);
            for (Values values : valuesToEmit) {
                collector.emit(values);
            }
        } catch (Exception e) {
            LOG.warn("Could not emit for row [" + rowKey + "] from Cassandra.", e);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.columnsMapper.declareOutputFields(declarer);

    }
}