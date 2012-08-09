package backtype.storm.contrib.cassandra.bolt.mapper;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class DefaultColumnsMapper implements ColumnsMapper {
    private static final long serialVersionUID = 1L;

    /**
     * Default behavior is to write each value in the tuple as a valueless column in Cassandra.
     * 
     * @param tuple
     * @return
     */
    public Map<String, String> mapToColumns(Tuple tuple){
        Fields fields = tuple.getFields();
        Map<String,String> columns = new HashMap<String,String>();
        for (int i=0; i < fields.size(); i++){
            columns.put(fields.get(i), "");
        }
        return columns;
    }
}
