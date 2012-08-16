// Copyright (c) 2012 P. Taylor Goetz

package backtype.storm.contrib.cassandra.bolt;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.ColumnsMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
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
public class DefaultLookupBolt extends BaseCassandraBolt implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLookupBolt.class);
    private ColumnFamily<String, String> columnFamily;
    private ColumnsMapper columnsMapper;

    public DefaultLookupBolt(TupleMapper tupleMapper, ColumnsMapper columnsMapper) {
        super(tupleMapper);
        this.columnsMapper = columnsMapper;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.columnFamily = new ColumnFamily<String, String>(tupleMapper.mapToColumnFamily(input), StringSerializer.get(),
                StringSerializer.get());
        String rowKey = tupleMapper.mapToRowKey(input);
        try {
            OperationResult<ColumnList<String>> result = this.keyspace.prepareQuery(this.columnFamily).getKey(rowKey).execute();
            ColumnList<String> columns = result.getResult();            
            List<Values> valuesToEmit = columnsMapper.mapToValues(rowKey, columns, input);
            for (Values values : valuesToEmit){
                collector.emit(values);
            }
        } catch (ConnectionException e) {
            LOG.warn("Could not emit for row [" + rowKey + "] from Cassandra.", e);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.columnsMapper.declareOutputFields(declarer);

    }
}