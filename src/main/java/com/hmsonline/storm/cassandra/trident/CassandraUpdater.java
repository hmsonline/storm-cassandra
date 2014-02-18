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
package com.hmsonline.storm.cassandra.trident;

import java.util.List;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class CassandraUpdater<K, C, V> extends BaseStateUpdater<CassandraState> {

    private static final long serialVersionUID = 1115563296010140546L;
    
    private TridentTupleMapper<K, C, V> tupleMapper;
    
    public CassandraUpdater(TridentTupleMapper<K, C, V> tupleMapper){
        this.tupleMapper = tupleMapper;
    }

    @Override
    public void updateState(CassandraState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.update(tuples, collector, this.tupleMapper);
    }

}
