package com.hmsonline.storm.cassandra.bolt.mapper;

import backtype.storm.tuple.Tuple;

public interface RangeQueryTupleMapper<K, C, V> extends TupleMapper<K, C, V> {

    /**
     * 
     * @param tuple
     * @return return String value of column for start range
     */
    public C mapToStartkey(Tuple tuple);

    /**
     * 
     * @param tuple
     * @return return String value of column for end range
     */
    public C mapToEndkey(Tuple tuple);
}
