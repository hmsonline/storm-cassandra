package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;

// TODO: Need to add generics everywhere instead of Strings
public interface TupleMapper<K,V> extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     * 
     * @param tuple
     * @return
     */
    public String mapToColumnFamily(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    public String mapToRowKey(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the columns
     * of data to write.
     * 
     * @param tuple
     * @return
     */
    public Map<K, V> mapToColumns(Tuple tuple);
}
