// Copyright (c) 2012 P. Taylor Goetz

package com.hmsonline.storm.cassandra.bolt;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.ColumnsMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.RangeQueryTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A bolt implementation that emits tuples based on a combination of cassandra
 * rowkey, columnkey, and delimiter.
 * <p/>
 * 
 * @author tgoetz
 */
@SuppressWarnings("serial")
public class CassandraLookupBolt<K,V> extends CassandraBolt<K,V> implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraLookupBolt.class);
    private ColumnsMapper<K,V> columnsMapper;
    private RangeQueryTupleMapper<K,V> queryTupleMapper = null;

    public CassandraLookupBolt(TupleMapper<K,V> tupleMapper, ColumnsMapper<K,V> columnsMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        super(tupleMapper, columnNameClass, columnValueClass);
        this.columnsMapper = columnsMapper;
    }

    public CassandraLookupBolt(RangeQueryTupleMapper<K,V> queryMapper, ColumnsMapper<K,V> columnsMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        super(queryMapper, columnNameClass, columnValueClass);
        this.queryTupleMapper = queryMapper;
        this.columnsMapper = columnsMapper;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String columnFamily = tupleMapper.mapToColumnFamily(input);
        String rowKey = tupleMapper.mapToRowKey(input);
        try {
            Columns<K, V> colMap = null;
            if (queryTupleMapper != null) {
                String start = queryTupleMapper.mapToStartkey(input);
                String end = queryTupleMapper.mapToEndkey(input);
                colMap = getClient().lookup(columnFamily, rowKey, start, end);
            } else {
                colMap = getClient().lookup(columnFamily, rowKey);
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