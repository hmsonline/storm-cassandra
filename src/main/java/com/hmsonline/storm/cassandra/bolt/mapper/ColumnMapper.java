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
package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *
 * @param <K>
 * @param <C>
 * @param <V>
 */
public interface ColumnMapper<K, C, V> extends Serializable {

    /**
     * Declares the fields produced by the bolt using this mapper.
     * 
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
    
    /**
     * Given a set of columns, maps to values to emit.
     * 
     * @param columns
     * @return
     */
    public List<Values> mapToValues(K rowKey, Map<C, V> columns, Tuple input);
}
