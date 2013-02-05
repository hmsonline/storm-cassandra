package com.hmsonline.storm.cassandra.bolt;

import java.util.HashMap;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

public class CompositeColumnTridentTupleMapper implements TridentTupleMapper<String, SimpleComposite, String>{

    @Override
    public String mapToColumnFamily(TridentTuple tuple) throws TupleMappingException {
        return "composite";
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

    @Override
    public Map<Object, Object> mapToEndKeyMap(TridentTuple tuple) throws TupleMappingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<Object, Object> mapToStartKeyMap(TridentTuple tuple) throws TupleMappingException {
        // TODO Auto-generated method stub
        return null;
    }

}
