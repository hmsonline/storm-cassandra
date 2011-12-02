/**
 * 
 */
package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Abstract <code>IRichBolt</code> implementation that caches/batches
 * <code>backtype.storm.tuple.Tuple</code> objects for a predetermined interval
 * (specified in milliseconds) and/or a maximum number of cached/batched tuples.
 * <p/>
 * When the batch interval or maximum batch size has been reached, the 
 * cached/batched <code>Tuple</code>
 * objects will be passed on to the subclass'
 * <code>executeBatch(List<Tuple> inputs)</code> method for processing.
 * <p/>
 * Subclasses are obligated to implement the <code>executeBatch(List<Tuple> inputs)</code> method,
 * called when a batch of tuples should be processed.
 * <p/>
 * Subclasses that overide the <code>prepare()</code> and <code>cleanup()</code>
 * methods <b><i>must</i></b> call the corresponding methods on the superclass (i.e. <code>super.prepare()</code> 
 * and <code>super.cleanup()</code> to ensure
 * proper initialization and termination.
 * 
 * 
 * 
 * @author tgoetz
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractBatchingBolt implements IRichBolt,
		CassandraConstants {
	
	private static final Logger LOG = LoggerFactory.getLogger(AbstractBatchingBolt.class);

	private static final long DEFAULT_FLUSH_INTERVAL = 1000;
	private static final long DEFAULT_MAX_BATCH_SIZE = 100;
	private Timer timer;
	private ConcurrentLinkedQueue<Tuple> batch;
	private long flushInterval = DEFAULT_FLUSH_INTERVAL;
	private long batchCount = 0;
	private long maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
	private Object lock;
	private boolean flushScheduled = false;
	
	/**
	 * Sets the interval (in milliseconds) with which the cache will be flushed.
	 * <p/>
	 * The bolt will continue to cache tuples until either <code>maxBatchSize</code>
	 * has been reached/exceeded, or the <code>flushInterval</code> has expired.
	 * @param flushInterval
	 */
	public void setFlushInterval(long flushInterval){
		this.flushInterval = flushInterval;
	}
	
	/**
	 * Sets the maximum number of tuples that will be cached before being flushed.
	 * <p/>
	 * The bolt will continue to cache tuples until either <code>maxBatchSize</code>
	 * has been reached/exceeded, or the <code>flushInterval</code> has expired.
	 * @param flushInterval
	 * @param batchSize
	 */
	public void setMaxBatchSize(long batchSize){
		this.maxBatchSize = batchSize;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.lock = new Object();
		this.batch = new ConcurrentLinkedQueue<Tuple>();
		this.timer = new Timer(true);

		this.timer.scheduleAtFixedRate(new FlushTask(), this.flushInterval, this.flushInterval);
	}

	@Override
	public final void execute(Tuple input) {
		synchronized (this.lock) {
			this.batch.offer(input);
			this.batchCount++;
		}
		if(this.batchCount > this.maxBatchSize && !this.flushScheduled){
			LOG.debug("Max reached. Scheduling flush.");
			this.flushScheduled = true;
			this.timer.schedule(new FlushTask(), 0);
		}
	}

	@Override
	public void cleanup() {
		this.timer.cancel();
		this.flush();
	}

	/**
	 * Process a <code>java.util.List</code> of <code>backtype.storm.tuple.Tuple</code> objects that
	 * have been cached/batched.
	 * <p/>
	 * This method is analagous to the <code>execute(Tuple input)</code> method defined in the bolt
	 * interface. Subclasses are responsible for processing and/or ack'ing tuples as necessary. The
	 * only difference is that tuples are passed in as a list, as opposed to one at a time.
	 * <p/>
	 * 
	 * 
	 * @param inputs
	 */
	public abstract void executeBatch(List<Tuple> inputs);
	

	private void flush() {
		LOG.debug("Flushing...");
		Tuple t = this.batch.poll();
		if (t == null) {
			return;
		} else {
			ArrayList<Tuple> localBatch = new ArrayList<Tuple>();
			LOG.debug("Creating batch...");
			synchronized (this.lock) {
				while (t != null) {
					localBatch.add(t);
					t = this.batch.poll();
				}
				this.batchCount = 0;
				this.flushScheduled = false;
			}
			this.executeBatch(localBatch);
			LOG.debug("Batch processed.");
		}
	}
	
	private class FlushTask extends TimerTask{
		@Override
		public void run() {
			flush();
		}
	}
}
