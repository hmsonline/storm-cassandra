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
import java.util.Map;

import backtype.storm.tuple.Tuple;

public interface TupleMapper<K, C, V> extends Serializable {

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the column
     * family to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToColumnFamily(Tuple tuple);
    
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the keyspace to write to.
     * 
     * @param tuple
     * @return
     */
    String mapToKeyspace(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra row
     * key.
     * 
     * @param tuple
     * @return
     */
    K mapToRowKey(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map the columns
     * of data to write.
     * 
     * @param tuple
     * @return
     */
    Map<C, V> mapToColumns(Tuple tuple);
    
    /**
     * Returns the row key class
     * 
     * @return
     */
    Class<K> getKeyClass();
    
    /**
     * Returns the column name class
     * 
     * @return
     */
    Class<C> getColumnNameClass();
    
    /**
     * Returns the column value class
     * 
     * @return
     */
    Class<V> getColumnValueClass();
}
