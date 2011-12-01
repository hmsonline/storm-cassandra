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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Abstract <code>IRichBolt</code> implementation that caches
 * <code>backtype.storm.tuple.Tuple</code> objects for a 
 * predetermined interval (specified in milliseconds).
 * <p/>
 * When the batch interval has been reached, the batched
 * <code>Tuple</code> objects will be passed on to the 
 * subclass' <code>executeBatch(List<Tuple> inputs)</code>
 * method for processing.
 * 
 * 
 * @author tgoetz
 *
 */
@SuppressWarnings("serial")
public abstract class AbstractBatchingBolt implements IRichBolt, CassandraConstants {
	
	private static final long DEFAULT_FLUSH_INTERVAL = 1000;
	private Timer timer;
	private ConcurrentLinkedQueue<Tuple> batch;
	private long flushInterval = DEFAULT_FLUSH_INTERVAL;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.batch = new ConcurrentLinkedQueue<Tuple>();
		this.timer = new Timer(true);
		
		this.timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				flush();
			}
		}, this.flushInterval, this.flushInterval);

	}


	@Override
	public final void execute(Tuple input) {
		this.batch.offer(input);
	}


	@Override
	public void cleanup() {
		this.timer.cancel();
		this.flush();
	}
	
	/**
	 * Process a <code>java.util.List</code> of <code>Tuple</code> objects that have
	 * been cached/batched.
	 * @param inputs
	 */
	public abstract void executeBatch(List<Tuple> inputs);
	
	private void flush(){
		Tuple t = this.batch.poll();
		if(t == null){
			return;
		} else{
			ArrayList<Tuple> localBatch = new ArrayList<Tuple>();
			while(t != null){
				localBatch.add(t);
				t=this.batch.poll();
			}
			this.executeBatch(localBatch);
		}	
	}
}
