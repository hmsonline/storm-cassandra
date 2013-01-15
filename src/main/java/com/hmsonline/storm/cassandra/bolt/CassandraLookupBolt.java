// Copyright (c) 2012 P. Taylor Goetz

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

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.ColumnsMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 * A bolt implementation that emits tuples based on a combination of cassandra
 * rowkey, columnkey, and delimiter.
 * <p/>
 * 
 * @author tgoetz
 */
@SuppressWarnings("serial")
public class CassandraLookupBolt<T> extends CassandraBolt<T> implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraLookupBolt.class);
    private ColumnsMapper<T> columnsMapper;
    private TupleMapper tupleMapper;

    public CassandraLookupBolt(TupleMapper<T> tupleMapper, ColumnsMapper<T> columnsMapper) {
        super(tupleMapper);
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
            Columns<T> colMap = this.cassandraClient.lookup(columnFamily, rowKey);
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