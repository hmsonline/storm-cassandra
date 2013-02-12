package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author tgoetz
 *
 * @param <K>
 * @param <C>
 * @param <V>
 */
public interface ColumnMapper<K, C, V> extends Serializable {

    /**
     * Declares the fields produced by the bolt using this mapper.
     * 
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
    
    /**
     * Given a set of columns, maps to values to emit.
     * 
     * @param columns
     * @return
     */
    public List<Values> mapToValues(K rowKey, Map<C, V> columns, Tuple input);
}
