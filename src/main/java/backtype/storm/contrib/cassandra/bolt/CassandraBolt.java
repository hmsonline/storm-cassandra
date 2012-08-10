package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.ColumnFamilyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.ColumnsMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultColumnFamilyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultColumnsMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultRowKeyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.RowKeyMapper;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class CassandraBolt extends BaseCassandraBolt implements IRichBolt, CassandraConstants, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);

    private OutputCollector collector;
    private boolean autoAck = true;
    private Fields declaredFields;
    
    public CassandraBolt(String columnFamily, String rowkeyField) {
        this(new DefaultColumnFamilyMapper(columnFamily), new DefaultRowKeyMapper(rowkeyField), new DefaultColumnsMapper());
    }

    public CassandraBolt(ColumnFamilyMapper cfDeterminable, RowKeyMapper rkDeterminable, ColumnsMapper colsDeterminable) {
        super(cfDeterminable, rkDeterminable, colsDeterminable);
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
            this.writeTuple(input);
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
