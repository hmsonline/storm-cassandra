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
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

public class SimpleTupleMapper implements TupleMapper<SimpleComposite, String, String> {
    
    private static final long serialVersionUID = 3535952327132771558L;
    private String columnFamily;
    private String keyspace;
    
    public SimpleTupleMapper(String keyspace, String columnFamily){
        this.columnFamily = columnFamily;
        this.keyspace = keyspace;
    }

    
    @Override
    public String mapToColumnFamily(Tuple tuple) {
        return this.columnFamily;
    }    
    @Override
    public String mapToKeyspace(Tuple tuple) {
        return this.keyspace;
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
