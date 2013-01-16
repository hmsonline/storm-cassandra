package com.hmsonline.storm.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 * Still WIP.
 * 
 * @author boneill42
 */
@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
public class TridentCassandraBatchBolt<K, V> extends TransactionalCassandraBatchBolt implements ITridentBatchBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraBatchBolt.class);
    private Object transactionId = null;

    public TridentCassandraBatchBolt(TupleMapper<K, V> tupleMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        super(tupleMapper, columnNameClass, columnValueClass);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector) {
        super.prepare(conf, context, collector, "TridentBolt");
    }

    @Override
    public void finishBatch() {
        List<Tuple> batch = new ArrayList<Tuple>();
        int size = queue.drainTo(batch);
        LOG.debug("Finishing batch for [" + transactionId + "], writing [" + size + "] tuples.");
        try {
            this.writeTuples(batch, this.tupleMapper);
        } catch (Exception e) {
            LOG.error("Could not write batch to cassandra.", e);
        }
    }

    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {
        super.execute(tuple);
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {
        super.finishBatch();
    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        // TODO Auto-generated method stub
        return null;
    }

}
