package com.hmsonline.storm.cassandra.bolt;

import java.util.HashMap;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Fields;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

public class SimpleTridentTupleMapper implements TridentTupleMapper<String, String, String> {

    private static final long serialVersionUID = 6362052836181968031L;
    
    private Fields fields;
    
    public SimpleTridentTupleMapper(Fields fields){
        this.fields = fields;
    }

    @Override
    public String mapToColumnFamily(TridentTuple tuple) throws TupleMappingException {
        return "trident";
    }

    @Override
    public String mapToRowKey(TridentTuple tuple) throws TupleMappingException {
        return tuple.getStringByField("key1");
    }

    @Override
    public Map<String, String> mapToColumns(TridentTuple tuple) throws TupleMappingException {
        HashMap<String, String> retval = new HashMap<String, String>();
        for(String field : this.fields.toList()){
            retval.put(field, tuple.getStringByField(field));
        }
        return retval;
    }

    @Override
    public String mapToEndKey(TridentTuple tuple) throws TupleMappingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String mapToStartKey(TridentTuple tuple) throws TupleMappingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean shouldDelete(TridentTuple tuple) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Class<String> getKeyClass() {
        return String.class;
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
