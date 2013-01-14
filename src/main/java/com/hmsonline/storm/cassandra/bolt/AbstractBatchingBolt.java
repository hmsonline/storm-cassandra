package com.hmsonline.storm.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * Abstract <code>IRichBolt</code> implementation that caches/batches
 * <code>backtype.storm.tuple.Tuple</code> and processes them on a separate
 * thread.
 * <p/>
 * <p/>
 * Subclasses are obligated to implement the
 * <code>executeBatch(List<Tuple> inputs)</code> method, called when a batch of
 * tuples should be processed.
 * <p/>
 * Subclasses that overide the <code>prepare()</code> and <code>cleanup()</code>
 * methods <b><i>must</i></b> call the corresponding methods on the superclass
 * (i.e. <code>super.prepare()</code> and <code>super.cleanup()</code>) to
 * ensure proper initialization and termination.
 * 
 * @author ptgoetz
 */
@SuppressWarnings("serial")
public abstract class AbstractBatchingBolt extends CassandraBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBatchingBolt.class);

    protected AckStrategy ackStrategy = AckStrategy.ACK_IGNORE;

    protected OutputCollector collector;

    protected LinkedBlockingQueue<Tuple> queue;

    private BatchThread batchThread;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context);
        int batchMaxSize = Utils.getInt(Utils.get(stormConf, CASSANDRA_BATCH_MAX_SIZE, 0));

        this.collector = collector;
        this.queue = new LinkedBlockingQueue<Tuple>();
        this.batchThread = new BatchThread(batchMaxSize);
        this.batchThread.start();
    }

    @Override
    public void execute(Tuple input) {
        if (this.ackStrategy == AckStrategy.ACK_ON_RECEIVE) {
            this.collector.ack(input);
        }
        this.queue.offer(input);
    }

    @Override
    public void cleanup() {
        this.batchThread.stopRunning();
        super.cleanup();
    }

    /**
     * Process a <code>java.util.List</code> of
     * <code>backtype.storm.tuple.Tuple</code> objects that have been
     * cached/batched.
     * <p/>
     * This method is analagous to the <code>execute(Tuple input)</code> method
     * defined in the bolt interface. Subclasses are responsible for processing
     * and/or ack'ing tuples as necessary. The only difference is that tuples
     * are passed in as a list, as opposed to one at a time.
     * <p/>
     * 
     * 
     * @param inputs
     */
    public abstract void executeBatch(List<Tuple> inputs);

    private class BatchThread extends Thread {

        int batchMaxSize;
        boolean stopRequested = false;

        BatchThread(int batchMaxSize) {
            super("batch-bolt-thread");
            super.setDaemon(true);
            this.batchMaxSize = batchMaxSize;
        }

        @Override
        public void run() {
            while (!stopRequested) {
                try {
                    ArrayList<Tuple> batch = new ArrayList<Tuple>();
                    // drainTo() does not block, take() does.
                    Tuple t = queue.take();
                    batch.add(t);
                    if (batchMaxSize > 0) {
                        queue.drainTo(batch, batchMaxSize);
                    } else {
                        queue.drainTo(batch);
                    }
                    executeBatch(batch);

                } catch (InterruptedException e) {
                    LOG.error("Interupted in batching bolt.", e);
                }
            }
        }

        synchronized void stopRunning() {
            this.stopRequested = true;
        }
    }

    public AckStrategy getAckStrategy() {
        return ackStrategy;
    }

    public void setAckStrategy(AckStrategy ackStrategy) {
        this.ackStrategy = ackStrategy;
    }
}
