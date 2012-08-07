package backtype.storm.contrib.cassandra.bolt.mapper;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface RowKeyMapper extends Serializable {
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    Object mapRowKey(Tuple tuple);
}
