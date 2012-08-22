package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.DefaultTupleMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DefaultBatchingCassandraBolt extends AbstractBatchingBolt implements CassandraConstants, Serializable {
    public DefaultBatchingCassandraBolt(TupleMapper tupleMapper) {
        super(tupleMapper);
    }

    public DefaultBatchingCassandraBolt(String columnFamily, String rowKeyField) {
        this(new DefaultTupleMapper(columnFamily, rowKeyField));
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBatchingCassandraBolt.class);

    @Override
    public void executeBatch(List<Tuple> inputs) {
        try {
            this.writeTuples(inputs);
            // NOTE: Changed this to ack on all or none since that is how the
            // mutation executes.
            if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
                for (Tuple tupleToAck : inputs) {
                    this.collector.ack(tupleToAck);
                }
            }
        } catch (Throwable e) {
            LOG.warn("Unable to write batch.", e);
            for (Tuple tupleToAck : inputs) {
                this.collector.fail(tupleToAck);
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // By default we don't emit anything.
    }
}
