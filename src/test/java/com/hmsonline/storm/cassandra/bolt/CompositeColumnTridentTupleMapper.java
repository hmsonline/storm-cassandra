/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
