package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.ColumnFamilyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.ColumnsMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultColumnFamilyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultColumnsMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.DefaultRowKeyMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.RowKeyMapper;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DefaultBatchingCassandraBolt extends BatchingCassandraBolt implements CassandraConstants, Serializable {
    public DefaultBatchingCassandraBolt(ColumnFamilyMapper cfDeterminable, RowKeyMapper rkDeterminable,
            ColumnsMapper colsDeterminable) {
        super(cfDeterminable, rkDeterminable, colsDeterminable);
    }

    public DefaultBatchingCassandraBolt(String columnFamily, String rowKey) {
        this(new DefaultColumnFamilyMapper(columnFamily), new DefaultRowKeyMapper(rowKey), 
                new DefaultColumnsMapper());
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBatchingCassandraBolt.class);

	@Override
	public void executeBatch(List<Tuple> inputs) {
		try {
			this.writeTuples(inputs);
			// NOTE: Changed this to ack on all or none since that is how the
			// mutation executes.
			if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
				for (Tuple tupleToAck : inputs) {
					this.collector.ack(tupleToAck);
				}
			}
		} catch (Throwable e) {
			LOG.warn("Unable to write batch.", e);
			for (Tuple tupleToAck : inputs) {
				this.collector.fail(tupleToAck);
			}
        }
    }
}
