package com.hmsonline.storm.cassandra.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.Equality;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class CassandraState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraState.class);
    public static final int DEFAULT_MAX_BATCH_SIZE = 3000;
    @SuppressWarnings("rawtypes")
    private AstyanaxClient client = null;
    private int maxBatchSize = 0;

    public CassandraState(AstyanaxClient<?, ?, ?> client) {
        this(client, DEFAULT_MAX_BATCH_SIZE);
    }

    public CassandraState(AstyanaxClient<?, ?, ?> client, int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        this.client = client;
        // this.mapper = mapper;
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("Begin Commit: {}", txid);
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("End Commit: {}", txid);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void update(List<TridentTuple> tuples, TridentCollector collector, TridentTupleMapper mapper) {
        LOG.debug("updating with {} tuples", tuples.size());

        try {
            if (this.maxBatchSize > 0) {
                int size = tuples.size();
                int batchCount = size / this.maxBatchSize;
                int count = 0;
                for (int i = 0; i < batchCount; i++) {
                    this.client.writeTuples(tuples.subList(i * this.maxBatchSize, (i + 1) * this.maxBatchSize), mapper);
                    count++;
                }
                this.client.writeTuples(tuples.subList(count * this.maxBatchSize, size), mapper);

                for (TridentTuple tuple : tuples) {
                    collector.emit(tuple.getValues());
                }
            } else {
                this.client.writeTuples(tuples, mapper);
            }

        } catch (Exception e) {
            LOG.warn("Batch write failed. Triggering replay.", e);
            throw new FailedException(e);
        }
    }
    
    
    public List<Map<?, ?>> batchRetrieve(List<TridentTuple> tuples, TridentTupleMapper tupleMapper){
        List<Map<?, ?>> retval = new ArrayList<Map<?, ?>>();
        for(TridentTuple input : tuples){
            
            try {
                Object start = tupleMapper.mapToStartKey(input);
                Object end = tupleMapper.mapToEndKey(input);
                List list = tupleMapper.mapToColumnsForLookup(input);

                Map<?, ?> colMap = null;
                
                // TODO break out different interfaces for different types of queries, then come back and fix this.
                if (list != null){
                    // slice query
                    colMap = client.lookup(tupleMapper, input, list);
                } else if (start != null && end != null){
                    // range query
                    colMap = client.lookup(tupleMapper, input, start, end, Equality.GREATER_THAN_EQUAL);
                } else {
                    // fetch by key
                    colMap = client.lookup(tupleMapper, input);                
                }
                retval.add(colMap);
            } catch (Exception e) {
                LOG.warn("Cassandra lookup failed.", e);
            }

        }
        return retval;
    }

}
