package backtype.storm.contrib.cassandra.bolt;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultTupleMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleCounterMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class CassandraCounterBatchingBolt extends AbstractBatchingBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory
			.getLogger(CassandraCounterBatchingBolt.class);

	private TupleCounterMapper tupleMapper;
	
	public CassandraCounterBatchingBolt(TupleCounterMapper tupleMapper) {
		this.tupleMapper = tupleMapper;
	}

	public CassandraCounterBatchingBolt(String columnFamily,
			String rowKeyField, String incrementAmountField) {
		this(new DefaultTupleCounterMapper(columnFamily, rowKeyField, incrementAmountField));
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
