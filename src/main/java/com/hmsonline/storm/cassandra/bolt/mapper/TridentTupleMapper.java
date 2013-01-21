package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import storm.trident.tuple.TridentTuple;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

public interface TridentTupleMapper<K, V> extends Serializable {
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToColumnFamily(TridentTuple tuple) throws TupleMappingException;

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    String mapToRowKey(TridentTuple tuple) throws TupleMappingException;

    Map<K, V> mapToColumns(TridentTuple tuple) throws TupleMappingException;

    Object mapToEndKey(TridentTuple tuple) throws TupleMappingException;

    Object mapToStartKey(TridentTuple tuple) throws TupleMappingException;

    boolean shouldDelete(TridentTuple tuple);
}
