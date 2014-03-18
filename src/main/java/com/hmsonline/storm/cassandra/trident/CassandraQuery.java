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
import java.util.Map;

import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;

import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class CassandraQuery extends BaseQueryFunction<CassandraState, Map<?,?>> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraQuery.class);
    
    private TridentColumnMapper columnsMapper;
    private TridentTupleMapper tupleMapper;
    private ExceptionHandler exceptionHandler;

    public CassandraQuery(TridentTupleMapper tupleMapper,
                          TridentColumnMapper columnMapper){
        this(tupleMapper, columnMapper, null);
    }

    public CassandraQuery(TridentTupleMapper tupleMapper,
            TridentColumnMapper columnMapper, ExceptionHandler exceptionHandler){
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public List<Map<?,?>> batchRetrieve(CassandraState state, List<TridentTuple> args) {
        return state.batchRetrieve(args, this.tupleMapper);
    }

    @Override
    public void execute(TridentTuple tuple, Map<?,?> result, TridentCollector collector) {
        try{
            List<Values> emitList = this.columnsMapper.mapToValues(this.tupleMapper.mapToRowKey(tuple), result, tuple);
            for(Values values : emitList){
                collector.emit(values);
            }
        } catch (Exception e){
            if(this.exceptionHandler != null){
                this.exceptionHandler.onException(e, collector);
            } else {
                LOG.warn("Error mapping columns to values.", e);
            }
        }
    }

}
