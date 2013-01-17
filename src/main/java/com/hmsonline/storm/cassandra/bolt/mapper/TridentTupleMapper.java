package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

public interface TridentTupleMapper<K, V> extends Serializable {
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToColumnFamily(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    String mapToRowKey(TridentTuple tuple);

    Map<K, V> mapToColumns(TridentTuple tuple);

    Object mapToEndKey(TridentTuple tuple);

    Object mapToStartKey(TridentTuple tuple);
}
