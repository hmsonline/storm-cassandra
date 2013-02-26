package com.hmsonline.storm.cassandra.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

@SuppressWarnings("serial")
public class CompositeColumnTridentTupleMapper implements TridentTupleMapper<String, SimpleComposite, String>{
    
    private String keyspace;
    public CompositeColumnTridentTupleMapper(String keyspace) {
        this.keyspace = keyspace;
    }
    
    @Override
    public String mapToColumnFamily(TridentTuple tuple) throws TupleMappingException {
        return "composite";
    }
    
    @Override
    public String mapToKeyspace(TridentTuple tuple) {
        return keyspace;        
    }

    @Override
    public String mapToRowKey(TridentTuple tuple) throws TupleMappingException {
        return tuple.getStringByField("rowkey");
    }

    @Override
    public Map<SimpleComposite, String> mapToColumns(TridentTuple tuple) throws TupleMappingException {
        Map<SimpleComposite, String> ret = new HashMap<SimpleComposite, String>();
        ret.put(new SimpleComposite(tuple.getStringByField("a"), tuple.getStringByField("b")), tuple.getStringByField("value"));
        return ret;
    }

    @Override
    public List<SimpleComposite> mapToColumnsForLookup(TridentTuple tuplex) {
        throw new RuntimeException("Should not be called since this mapper is not used to read data.");
    }
    
    @Override
    public SimpleComposite mapToEndKey(TridentTuple tuple) throws TupleMappingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleComposite mapToStartKey(TridentTuple tuple) throws TupleMappingException {
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
    public Class<SimpleComposite> getColumnNameClass() {
        return SimpleComposite.class;
    }

    @Override
    public Class<String> getColumnValueClass() {
        return String.class;
    }

}
