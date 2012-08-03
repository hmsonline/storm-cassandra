package backtype.storm.contrib.cassandra.bolt.mapper;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface ColumnFamilyMapper extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the
     * column family to write to.
     * 
     * @param tuple
     * @return
     */
    public String mapToColumnFamily(Tuple tuple);
}
