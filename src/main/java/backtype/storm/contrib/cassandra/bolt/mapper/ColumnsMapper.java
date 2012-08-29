package backtype.storm.contrib.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.netflix.astyanax.model.ColumnList;

/**
 * Maps a list of columns to a set of tuples.
 * 
 * @author boneill42
 */
public interface ColumnsMapper extends Serializable {

    /**
     * Declares the fields produced by the bolt using this mapper.
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer);

    
    /**
     * Given a set of columns, maps to values to emit.
     * @param columns
     * @return
     */
    public List<Values> mapToValues(String rowKey, ColumnList<String> columns, Tuple input);

}
