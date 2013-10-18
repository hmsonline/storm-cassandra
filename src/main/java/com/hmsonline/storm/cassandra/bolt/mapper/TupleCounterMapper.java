package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

/**
 * Interface for mapping storm Tuples to Cassandra counter columns.
 * 
 * @author tgoetz
 *
 */
public interface TupleCounterMapper<K, C> extends Serializable {

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
     * Given a <code>backtype.storm.tuple.Tuple</code> return the amount that
     * must be incremented by.
     * 
     * @param tuple
     * @return
     */
    long mapToIncrementAmount(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the columns
     * that must be incremented by the increment amount.
     * 
     * @param tuple
     * @return
     */
    List<C> mapToColumnList(Tuple tuple);
    
    /**
     * Returns the row key class
     * 
     * @return
     */
    Class<K> getKeyClass();
    
    /**
     * Returns the column name class
     * 
     * @return
     */
    Class<C> getColumnNameClass();
    
}
