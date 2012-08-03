package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.ColumnsDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnsDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultRowKeyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.RowKeyDeterminable;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DefaultBatchingCassandraBolt extends BatchingCassandraBolt implements CassandraConstants, Serializable {
    public DefaultBatchingCassandraBolt(ColumnFamilyDeterminable cfDeterminable, RowKeyDeterminable rkDeterminable,
            ColumnsDeterminable colsDeterminable) {
        super(cfDeterminable, rkDeterminable, colsDeterminable);
    }

    public DefaultBatchingCassandraBolt(String columnFamily, String rowKey) {
        this(new DefaultColumnFamilyDeterminable(columnFamily), new DefaultRowKeyDeterminable(rowKey), 
                new DefaultColumnsDeterminable());
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBatchingCassandraBolt.class);

    @Override
    public void executeBatch(List<Tuple> inputs) {
        LOG.info("Executing batch [" + inputs.size() + "]");
        try {
            this.writeTuples(inputs);
            if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
                for (Tuple tupleToAck : inputs) {
                    this.collector.ack(tupleToAck);
                }
            }
        } catch (Throwable e) {
            LOG.error("Unable to write batch.", e);
        }
    }
}
