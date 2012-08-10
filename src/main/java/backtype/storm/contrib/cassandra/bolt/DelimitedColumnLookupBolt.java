// Copyright (c) 2012 P. Taylor Goetz

package backtype.storm.contrib.cassandra.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.DefaultColumnFamilyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultColumnsMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultRowKeyMapper;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * A bolt implementation that emits tuples based on a combination of cassandra
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
 */
@SuppressWarnings("serial")
public class DelimitedColumnLookupBolt extends BaseCassandraBolt implements IBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DelimitedColumnLookupBolt.class);
    private ColumnFamily<String, String> columnFamily;
    private String rowKeyField;
    private String columnKeyField;
    private String delimiter;
    private String columnFamilyName;

    private String emitIdFieldName;
    private String emitValueFieldName;

    private boolean isDrpc = false;

    public DelimitedColumnLookupBolt(String columnFamily, String rowKeyField, String columnKeyField, String delimiter,
            String emitIdFieldName, String emitValueFieldName, boolean isDrpc) {
        super(new DefaultColumnFamilyMapper(columnFamily), new DefaultRowKeyMapper(rowKeyField), 
                new DefaultColumnsMapper());

        this.columnFamilyName = columnFamily;
        this.rowKeyField = rowKeyField;
        this.columnKeyField = columnKeyField;
        this.delimiter = delimiter;
        this.emitIdFieldName = emitIdFieldName;
        this.emitValueFieldName = emitValueFieldName;
        this.isDrpc = isDrpc;
    }

    public DelimitedColumnLookupBolt(String columnFamily, String rowKeyField, String columnKeyField, String delimiter,
            String emitIdFieldName, String emitValueFieldName) {
        this(columnFamily, rowKeyField, columnKeyField, delimiter, emitIdFieldName, emitValueFieldName, false);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        this.columnFamily = new ColumnFamily<String, String>(this.columnFamilyName, StringSerializer.get(),
                StringSerializer.get());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String rowKey = input.getStringByField(this.rowKeyField);
        try {
            OperationResult<ColumnList<String>> result = this.keyspace.prepareQuery(this.columnFamily).getKey(rowKey)
                    .execute();
            ColumnList<String> columns = result.getResult();
            String delimVal = columns.getStringValue(this.columnKeyField, "");
            if (delimVal != null) {
                String[] vals = delimVal.split(this.delimiter);
                for (String val : vals) {
                    if (this.isDrpc) {
                        collector.emit(new Values(input.getValue(0), rowKey, val));
                    } else {
                        collector.emit(new Values(rowKey, val));
                    }
                }
            }
        } catch (ConnectionException e) {
            LOG.warn("Could emit for row [" + rowKey + "] from Cassandra.");
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.isDrpc) {
            declarer.declare(new Fields("id", this.emitIdFieldName, this.emitValueFieldName));
        } else {
            declarer.declare(new Fields(this.emitIdFieldName, this.emitValueFieldName));
        }

    }
}