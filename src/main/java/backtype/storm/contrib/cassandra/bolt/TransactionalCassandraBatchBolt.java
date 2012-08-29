package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings({ "serial", "rawtypes" })
public class TransactionalCassandraBatchBolt extends CassandraBatchingBolt implements IBatchBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalCassandraBatchBolt.class);

    public TransactionalCassandraBatchBolt(TupleMapper tupleMapper) {
        super(tupleMapper);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        this.queue = new LinkedBlockingQueue<Tuple>();        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // By default we don't emit anything.        
    }

    @Override
    public void finishBatch() {
        List<Tuple> batch = new ArrayList<Tuple>();
        queue.drainTo(batch);
        try {
            this.writeTuples(batch);
        } catch (ConnectionException e) {
            LOG.error("Could not write batch to cassandra.", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        queue.add(tuple);        
    }
}
