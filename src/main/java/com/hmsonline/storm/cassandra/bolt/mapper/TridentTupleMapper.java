package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;

import storm.trident.tuple.TridentTuple;


public interface TridentTupleMapper<T> extends Serializable {
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     *
     * @param tuple
     * @return
     */
    public String mapToColumnFamily(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     *
     * @param tuple
     * @return
     */
    public String mapToRowKey(TridentTuple tuple);

    public Map<T, String> mapToColumns(TridentTuple tuple);

    public Object mapToEndKey(TridentTuple tuple);

    public Object mapToStartKey(TridentTuple tuple);
}
