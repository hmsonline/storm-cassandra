package backtype.storm.contrib.cassandra.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.ColumnsDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnsDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultRowKeyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.RowKeyDeterminable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class CassandraBolt extends BaseCassandraBolt implements IRichBolt, CassandraConstants {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);

    private OutputCollector collector;
    private boolean autoAck = true;

    private Fields declaredFields;

    private ColumnFamilyDeterminable cfDeterminable;
    private RowKeyDeterminable rkDeterminable;
    private ColumnsDeterminable colsDeterminable;
    
    public CassandraBolt(String columnFamily, String rowkeyField) {
        this(new DefaultColumnFamilyDeterminable(columnFamily), new DefaultRowKeyDeterminable(rowkeyField), new DefaultColumnsDeterminable());
    }

    public CassandraBolt(ColumnFamilyDeterminable cfDeterminable, RowKeyDeterminable rkDeterminable, ColumnsDeterminable colsDeterminable) {
        this.cfDeterminable = cfDeterminable;
        this.rkDeterminable = rkDeterminable;
    }

    /*
     * IRichBolt Implementation
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context);
        LOG.debug("Preparing...");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("Tuple received: " + input);
        try {
            this.writeTuple(input, this.cfDeterminable, this.rkDeterminable, this.colsDeterminable);
            if (this.autoAck) {
                this.collector.ack(input);
            }
        } catch (Throwable e) {
            LOG.warn("Caught throwable.", e);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.declaredFields != null) {
            declarer.declare(this.declaredFields);
        }

    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}
