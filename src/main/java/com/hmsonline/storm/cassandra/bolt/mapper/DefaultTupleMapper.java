package com.hmsonline.storm.cassandra.bolt.mapper;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class DefaultTupleMapper implements TupleMapper<String, String, String> {
    private static final long serialVersionUID = 1L;
    private String rowKeyField;
    private String columnFamily;

    /**
     * Construct default mapper.
     * 
     * @param columnFamily
     *            column family to write to.
     * @param rowKeyField
     *            tuple field to use as the row key.
     */
    public DefaultTupleMapper(String columnFamily, String rowKeyField) {
        this.rowKeyField = rowKeyField;
        this.columnFamily = columnFamily;
    }

    @Override
    public String mapToRowKey(Tuple tuple) {
        return tuple.getValueByField(this.rowKeyField).toString();
    }

    /**
     * Default behavior is to write each value in the tuple as a key:value pair
     * in the Cassandra row.
     * 
     * @param tuple
     * @return
     */
    @Override
    public Map<String, String> mapToColumns(Tuple tuple) {
        Fields fields = tuple.getFields();
        Map<String, String> columns = new HashMap<String, String>();
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i);
            Object value = tuple.getValueByField(name);
            columns.put(name, (value != null ? value.toString() : ""));
        }
        return columns;
    }

    @Override
    public String mapToColumnFamily(Tuple tuple) {
        return this.columnFamily;
    }

    @Override
    public Class<String> getKeyClass() {
        // TODO Auto-generated method stub
        return String.class;
    }

    @Override
    public Class<String> getColumnNameClass() {
        // TODO Auto-generated method stub
        return String.class;
    }

    @Override
    public Class<String> getColumnValueClass() {
        // TODO Auto-generated method stub
        return String.class;
    }
    
    
}
