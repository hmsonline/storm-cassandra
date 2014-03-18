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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * For counter columns, an increment amount is required, the following fields
 * must be specified: rowKey, and incrementAmount, all remaining fields are
 * assumed to be column names to be incremented by the specified amount.
 */
public class DefaultTupleCounterMapper implements TupleCounterMapper<String,String> {

    private static final long serialVersionUID = 1L;
    private String rowKeyField;
    private String columnFamily;
    private String incrementAmountField;
    private String keyspace;

    public DefaultTupleCounterMapper(String keyspace, String columnFamily, String rowKeyField, String incrementAmountField) {
        this.columnFamily = columnFamily;
        this.rowKeyField = rowKeyField;
        this.incrementAmountField = incrementAmountField;
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
    public String mapToRowKey(Tuple tuple) {
        return tuple.getValueByField(this.rowKeyField).toString();
    }

    @Override
    public long mapToIncrementAmount(Tuple tuple) {
        return tuple.getLongByField(incrementAmountField);
    }

    @Override
    public List<String> mapToColumnList(Tuple tuple) {
        Fields fields = tuple.getFields();
        List<String> result = new ArrayList<String>();
        Iterator<String> it = fields.iterator();
        while (it.hasNext()) {
            String fieldName = it.next();
            if (!fieldName.equals(rowKeyField) && !fieldName.equals(incrementAmountField))
                result.add(tuple.getValueByField(fieldName).toString());
        }
        return result;
    }

	@Override
	public Class<String> getKeyClass() {
		return String.class;
	}

	@Override
	public Class<String> getColumnNameClass() {
		return String.class;
	}

}
