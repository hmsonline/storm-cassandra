package backtype.storm.contrib.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public interface ColumnsMapper extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the columns of data to write.
     * 
     * @param tuple
     * @return
     */
    // TODO: Need to add generics everywhere instead of Strings
    public Map<String, String> mapToColumns(Tuple tuple);
}
