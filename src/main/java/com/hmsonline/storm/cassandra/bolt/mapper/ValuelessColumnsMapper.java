package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.mortbay.log.Log;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A mapper implementation that emits tuples based on a combination of cassandra
 * rowkey, collumnkey, and delimiter.
 * <p/>
 * When this bolt receives a tuple, it will attempt the following:
 * <ol>
 * <li>Look up a value in the tuple using <code>rowKeyField</code></li>
 * <li>Fetch the corresponding row from cassandra</li>
 * <li>Fetch the column <code>columnKeyField</code> value from the row.</li>
 * <li>Split the column value into an array based on <code>delimiter</code></li>
 * <li>For each value, emit a tuple with <code>{emitIdFieldName}={value}</code></li>
 * </ol>
 * For example, given the following cassandra row: <br/>
 * 
 * <pre>
 * RowKey: mike
 * => (column=followers, value=john:bob, timestamp=1328848653055000)
 * </pre>
 * 
 * and the following bolt setup:
 * 
 * <pre>
 * rowKeyField = "rowKey"
 * columnKeyField = "followers"
 * delimiter = ":"
 * emitIdFieldName = "rowKey"
 * emitValueFieldName = "follower"
 * </pre>
 * 
 * if the following tuple were received by the bolt:
 * 
 * <pre>
 * {rowKey:mike}
 * </pre>
 * 
 * The following tuples would be emitted:
 * 
 * <pre>
 * {rowKey:mike, follower:john}
 * {rowKey:mike, follower:bob}
 * </pre>
 * 
 * @author tgoetz
 * @author boneill42
 */
@SuppressWarnings("serial")
public class ValuelessColumnsMapper implements ColumnsMapper<String, String>, Serializable {
    private String emitFieldForRowKey;
    private String emitFieldForColumnName;
    private boolean isDrpc;

    /**
     * Constructs a ValuelessColumnsMapper
     * 
     * @param emitFieldForRowKey
     *            This is the field name for the rowkey in the outbound tuple(s)
     * @param emitFieldForColumnName
     *            This is the field name for column names in the outbound
     *            tuple(s)
     * @param isDrpc
     */
    public ValuelessColumnsMapper(String emitFieldForRowKey, String emitFieldForColumnName, boolean isDrpc) {
        this.isDrpc = isDrpc;
        this.emitFieldForRowKey = emitFieldForRowKey;
        this.emitFieldForColumnName = emitFieldForColumnName;
    }

    /**
     * Declares the fields produced by this bolt.
     * 
     * 
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.isDrpc) {
            declarer.declare(new Fields("id", this.emitFieldForRowKey, this.emitFieldForColumnName));
        } else {
            declarer.declare(new Fields(this.emitFieldForRowKey, this.emitFieldForColumnName));
        }
    }

    /**
     * Given a set of columns, maps to values to emit.
     * 
     * @param columns
     * @return
     */
    @Override
    public List<Values> mapToValues(String rowKey, Columns<String, String> columns, Tuple input) {
        List<Values> values = new ArrayList<Values>();        
        for(Column<String,String> column : columns) {
        	String columnName = column.getKey();
            if (this.isDrpc) {
                values.add(new Values(input.getValue(0), rowKey, columnName));
            } else {
                values.add(new Values(rowKey, columnName));
            }
        }
        return values;
    }
}
