package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import storm.trident.tuple.TridentTuple;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

public interface TridentTupleMapper<K, C, V> extends Serializable {
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
    K mapToRowKey(TridentTuple tuple) throws TupleMappingException;

    Map<C, V> mapToColumns(TridentTuple tuple) throws TupleMappingException;

    C mapToEndKey(TridentTuple tuple) throws TupleMappingException;

    C mapToStartKey(TridentTuple tuple) throws TupleMappingException;

    Map<Object, Object> mapToEndKeyMap(TridentTuple tuple) throws TupleMappingException;

    Map<Object, Object> mapToStartKeyMap(TridentTuple tuple) throws TupleMappingException;

    boolean shouldDelete(TridentTuple tuple);

    Class<K> getKeyClass();

    Class<C> getColumnNameClass();

    Class<V> getColumnValueClass();
}
