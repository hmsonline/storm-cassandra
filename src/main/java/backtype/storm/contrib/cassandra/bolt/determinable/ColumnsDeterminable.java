package backtype.storm.contrib.cassandra.bolt.determinable;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public interface ColumnsDeterminable extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, determine the columns of data to write.
     * 
     * @param tuple
     * @return
     */
    // TODO: Need to add generics everywhere instead of Strings
    public Map<String, String> determineColumns(Tuple tuple);
}
