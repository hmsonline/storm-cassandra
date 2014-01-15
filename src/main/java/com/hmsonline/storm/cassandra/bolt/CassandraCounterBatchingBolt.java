package com.hmsonline.storm.cassandra.bolt;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;

public class CassandraCounterBatchingBolt<K, C, V> extends AbstractBatchingBolt<K, C, V> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCounterBatchingBolt.class);

    private TupleCounterMapper<K, C> tupleMapper;

    public CassandraCounterBatchingBolt(String clientConfigKey, TupleCounterMapper<K, C> tupleMapper) {
        super(clientConfigKey, null);
        this.tupleMapper = tupleMapper;
    }

    public CassandraCounterBatchingBolt(String keyspace, String clientConfigKey, String columnFamily, String rowKeyField, String incrementAmountField) {
        this(clientConfigKey, (TupleCounterMapper<K, C>) new DefaultTupleCounterMapper(keyspace, columnFamily, rowKeyField, incrementAmountField));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // we don't emit anything from here.
    }

    @Override
    public void executeBatch(List<Tuple> inputs) {
        try {
            this.incrementCounters(inputs, tupleMapper);
            // NOTE: Changed this to ack on all or none since that is how the
            // mutation executes.
            if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
                for (Tuple tupleToAck : inputs) {
                    this.collector.ack(tupleToAck);
                }
            }
        } catch (Throwable e) {
            LOG.error("Unable to write batch.", e);
            for (Tuple tupleToAck : inputs) {
                this.collector.fail(tupleToAck);
            }
        }
    }

}
