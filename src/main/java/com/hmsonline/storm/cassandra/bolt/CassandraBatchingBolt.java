package com.hmsonline.storm.cassandra.bolt;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 * This is a batching bolt that can be used outside of a transactional topology.
 * It does *not* implement IBatchBolt for that reason. If you want to use the
 * batching inside a transactional topology, use
 * <code>TransactionBatchingCassandraBolt</code>.
 * 
 * @author boneill42
 */
@SuppressWarnings("serial")
public class CassandraBatchingBolt<K,V> extends AbstractBatchingBolt<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBatchingBolt.class);
    
    public CassandraBatchingBolt(TupleMapper<K,V> tupleMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        super(tupleMapper, columnNameClass, columnValueClass);
    }
    
    @Override
    public void executeBatch(List<Tuple> inputs) {
        try {
            this.writeTuples(inputs, tupleMapper);
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

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // By default we don't emit anything.
    }

}
