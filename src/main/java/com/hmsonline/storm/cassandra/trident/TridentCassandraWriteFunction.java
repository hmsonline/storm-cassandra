package com.hmsonline.storm.cassandra.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.exceptions.StormCassandraException;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

public class TridentCassandraWriteFunction<K, C, V> implements Function {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraWriteFunction.class);
    protected TridentTupleMapper<K, C, V> tupleMapper;
    private AstyanaxClient<K, C, V> client;

    private String clientConfigKey;
    private Object valueToEmit;
    
    public void setValueToEmitAfterWrite(Object valueToEmit) {
        this.valueToEmit = valueToEmit;
    }

    public TridentCassandraWriteFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper) {
        this.tupleMapper = tupleMapper;
        this.clientConfigKey = clientConfigKey;
        this.valueToEmit = null;
    }
    public TridentCassandraWriteFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper,
            Object valueToEmit) {
        this(clientConfigKey, tupleMapper);
        this.valueToEmit = valueToEmit;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void prepare(Map stormConf, TridentOperationContext context) {
        Map<String, Object> config = (Map<String, Object>) stormConf.get(this.clientConfigKey);
        client = new AstyanaxClient<K, C, V>();
        client.start(config);
    }

    @Override
    public void cleanup() {
        this.client.stop();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            writeTuple(tuple);
            if (this.valueToEmit != null) {
                collector.emit(new Values(this.valueToEmit));
            }
        } catch (TupleMappingException e) {
            LOG.error("Skipping tuple: " + tuple, e);
        } catch (StormCassandraException e) {
            LOG.error("Failed to write tuple. Exception: " + e.getLocalizedMessage());
            // This will tell storm to replay the whole batch
            // TODO should we add a number of retry here?
            throw new FailedException();
        } catch (Exception e) {
            LOG.error("Unexcepted exception: " + e.getLocalizedMessage());
            // unexpected error should not be replayed. Log only
            collector.reportError(e);
        }
    }

    public void writeTuple(TridentTuple input) throws Exception {
        this.client.writeTuple(input, this.tupleMapper);
    }
}
