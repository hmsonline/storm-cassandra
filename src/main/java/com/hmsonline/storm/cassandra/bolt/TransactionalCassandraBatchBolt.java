package com.hmsonline.storm.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings({ "serial", "rawtypes" })
public class TransactionalCassandraBatchBolt<T> extends CassandraBatchingBolt<T> implements IBatchBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalCassandraBatchBolt.class);
    private Object transactionId = null;

    public TransactionalCassandraBatchBolt(TupleMapper<T> tupleMapper) {
        super(tupleMapper);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        super.prepare(conf, context);
        this.queue = new LinkedBlockingQueue<Tuple>();  
        this.transactionId = id;
        LOG.debug("Preparing cassandra batch [" + transactionId + "]");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // By default we don't emit anything.        
    }

    @Override
    public void execute(Tuple tuple) {
        // LOG.debug("Executing tuple for [" + transactionId + "]");
        queue.add(tuple);        
    }
    
    @Override
    public void finishBatch() {
        
        List<Tuple> batch = new ArrayList<Tuple>();
        int size = queue.drainTo(batch);
        LOG.debug("Finishing batch for [" + transactionId + "], writing [" + size + "] tuples.");
        try {
            this.writeTuples(batch, tupleMapper);
        } catch (Exception e) {
            LOG.error("Could not write batch to cassandra.", e);
        }
    }

}
