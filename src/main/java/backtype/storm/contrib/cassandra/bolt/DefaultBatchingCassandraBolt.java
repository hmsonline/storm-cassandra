package backtype.storm.contrib.cassandra.bolt;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultRowKeyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.RowKeyDeterminable;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DefaultBatchingCassandraBolt extends BatchingCassandraBolt
		implements CassandraConstants {
	private RowKeyDeterminable rkDeterminable;

	public DefaultBatchingCassandraBolt(
			ColumnFamilyDeterminable cfDeterminable,
			RowKeyDeterminable rkDeterminable) {
		super(cfDeterminable);
		this.rkDeterminable = rkDeterminable;
	}

	public DefaultBatchingCassandraBolt(String columnFamily, String rowKey) {
		this(new DefaultColumnFamilyDeterminable(columnFamily),
				new DefaultRowKeyDeterminable(rowKey));
	}

	private static final Logger LOG = LoggerFactory
			.getLogger(DefaultBatchingCassandraBolt.class);

	@Override
	public void executeBatch(List<Tuple> inputs) {
		try {
			this.writeTuples(inputs, this.cfDeterminable, this.rkDeterminable);
			// NOTE: Changed this to ack on all or none since that is how the
			// mutation executes.
			if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
				for (Tuple tupleToAck : inputs) {
					this.collector.ack(tupleToAck);
				}
			}
		} catch (Throwable e) {
			LOG.warn("Unable to write batch.", e);
		}
	}
}
