package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public interface TupleMapper<K, C, V> extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToColumnFamily(Tuple tuple);
    
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the keyspace to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToKeyspace(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    K mapToRowKey(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the columns
     * of data to write.
     * 
     * @param tuple
     * @return
     */
    Map<C, V> mapToColumns(Tuple tuple);
    
    Class<K> getKeyClass();
    
    Class<C> getColumnNameClass();
    
    Class<V> getColumnValueClass();
}
