package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

public interface TupleCounterMapper extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToColumnFamily(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    String mapToRowKey(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> returns the amount that
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
    List<String> mapToColumnList(Tuple tuple);

}
