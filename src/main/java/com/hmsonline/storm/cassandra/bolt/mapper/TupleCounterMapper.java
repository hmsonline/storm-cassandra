package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

// TODO: Need to add generics everywhere instead of Strings
public interface TupleCounterMapper extends Serializable {

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
     * Given a <code>backtype.storm.tuple.Tuple</code> returns the amount that 
     * must be incremented by.
     * 
     * @param tuple
     * @return
     */
    public long mapToIncrementAmount(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the columns
     * that must be incremented by the increment amount.
     * 
     * @param tuple
     * @return
     */
    public List<String> mapToColumnList(Tuple tuple);

}
