package com.hmsonline.storm.cassandra.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

public class SimpleTupleMapper implements TupleMapper<SimpleComposite, String, String> {
    
    private static final long serialVersionUID = 3535952327132771558L;
    private String columnFamily;
    
    public SimpleTupleMapper(String columnFamily){
        this.columnFamily = columnFamily;
    }

    
    @Override
    public String mapToColumnFamily(Tuple tuple) {
        return this.columnFamily;
    }
    @Override
    public SimpleComposite mapToRowKey(Tuple tuple) {
        return new SimpleComposite(tuple.getStringByField("key1"), tuple.getStringByField("key2"));
    }
    @Override
    public Map<String, String> mapToColumns(Tuple tuple) {
        HashMap<String, String> map = new HashMap<String, String>();
        Fields fields = tuple.getFields();
        for(int i = 0;i<fields.size();i++){
            map.put(fields.get(i), tuple.getStringByField(fields.get(i)));
        }
        return map;
    }
    @Override
    public Class<SimpleComposite> getKeyClass() {
        return SimpleComposite.class;
    }
    @Override
    public Class<String> getColumnNameClass() {
        return String.class;
    }
    @Override
    public Class<String> getColumnValueClass() {
        return String.class;
    }



}
