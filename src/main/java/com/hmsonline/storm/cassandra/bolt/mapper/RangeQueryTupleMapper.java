package com.hmsonline.storm.cassandra.bolt.mapper;

import backtype.storm.tuple.Tuple;

public interface RangeQueryTupleMapper<T> extends TupleMapper<T> {

    /**
     *
     * @param tuple
     * @return return String value of column for start range
     */
    public String mapToStartkey(Tuple tuple);

    /**
     *
     * @param tuple
     * @return return String value of column for end range
     */
    public String mapToEndkey(Tuple tuple);
}
