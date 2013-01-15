package com.hmsonline.storm.cassandra.bolt.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A mapper implementation that emits tuples based on a combination of cassandra
 * rowkey, columnkey, and delimiter.
 * <p/>
 * When this bolt received a tuple, it will attempt the following:
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
public class DelimitedColumnsMapper implements ColumnsMapper<String>, Serializable {
    private String columnKeyField;
    private String emitIdFieldName;
    private String emitValueFieldName;
    private String delimiter;
    private boolean isDrpc;

    public DelimitedColumnsMapper(String columnKeyField, String delimiter, String emitIdFieldName,
            String emitValueFieldName, boolean isDrpc) {
        this.columnKeyField = columnKeyField;
        this.delimiter = delimiter;
        this.isDrpc = isDrpc;
        this.emitIdFieldName = emitIdFieldName;
        this.emitValueFieldName = emitValueFieldName;
    }

    /**
     * Declares the fields produced by this bolt.
     * 
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.isDrpc) {
            declarer.declare(new Fields("id", this.emitIdFieldName, this.emitValueFieldName));
        } else {
            declarer.declare(new Fields(this.emitIdFieldName, this.emitValueFieldName));
        }
    }

    /**
     * Given a set of columns, maps to values to emit.
     * 
     * @param columns
     * @return
     */
    @Override
    public List<Values> mapToValues(String rowKey, Columns<String> columns, Tuple input) {
        List<Values> values = new ArrayList<Values>();
        String delimVal = columns.getColumnValue(this.columnKeyField);
        if (delimVal != null) {
            String[] vals = delimVal.split(this.delimiter);
            for (String val : vals) {
                if (this.isDrpc) {
                    values.add(new Values(input.getValue(0), rowKey, val));
                } else {
                    values.add(new Values(rowKey, val));
                }
            }
        }
        return values;
    }
}
