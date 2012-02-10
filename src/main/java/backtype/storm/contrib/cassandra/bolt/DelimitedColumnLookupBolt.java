// Copyright (c) 2012 Health Market Science, Inc.

package backtype.storm.contrib.cassandra.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


/**
 * A bolt implementation that emits tuples based on a combination of cassandra
 * rowkey, collumnkey, and delimiter.
 * <p/>
 * When this bolt received a tuple, it will attempt the following:
 * <ol>
 * <li>Look up a value in the tuple using <code>rowKeyField</code></li>
 * <li>Fetch the corresponding row from cassandra</li>
 * <li>Fetch the column <code>columnKeyField</code> value from the row.</li>
 * <li>Split the column value into an array based on <code>delimiter</code></li>
 * <li>For each value, emit a tuple with <code>{emitIdFieldName}={value}</code></li>
 * </ol>
 * For example, given the following cassandra row:
 * <br/>
 * <pre>
 * RowKey: mike
 * => (column=followers, value=john:bob, timestamp=1328848653055000)
 * </pre>
 * 
 * and the following bolt setup:
 * <pre>
 * rowKeyField = "id"
 * columnKeyField = "followers"
 * delimiter = ":"
 * emitIdFieldName = "id"
 * emitValueFieldName = "follower"
 * </pre>
 * 
 * if the following tuple were received by the bolt:
 * <pre>
 * {id:mike}
 * </pre>
 * 
 * The following tuples would be emitted:
 * <pre>
 * {id:mike, follower:john}
 * {id:mike, follower:bob}
 * </pre>
 * 
 * 
 * @author tgoetz
 *
 */
@SuppressWarnings("serial")
public class DelimitedColumnLookupBolt extends BaseCassandraBolt {

    private String rowKeyField;
    private String columnKeyField;
    private String delimiter;

    private String emitIdFieldName;
    private String emitValueFieldName;

    private String[] declaredFields;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        String emitIdFieldValue = input.getStringByField(this.emitIdFieldName);
//        String delimitedValueKey = 
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    /**
     * foo
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.declaredFields));
    }

}
