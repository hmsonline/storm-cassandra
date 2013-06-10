package com.hmsonline.storm.cassandra.trident;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.FailedException;

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
	@SuppressWarnings("rawtypes")
	private TridentTupleMapper mapper = null;
	
	private int maxBatchSize = 0;
	
	public CassandraState(AstyanaxClient<?, ?, ?> client, TridentTupleMapper<?, ?, ?> mapper){
		this(client, mapper, DEFAULT_MAX_BATCH_SIZE);
	}
	
	public CassandraState(AstyanaxClient<?, ?, ?> client, TridentTupleMapper<?, ?, ?> mapper, int maxBatchSize){
		this.maxBatchSize = maxBatchSize;
		this.client = client;
		this.mapper = mapper;
	}

	@Override
	public void beginCommit(Long txid) {
		LOG.debug("Begin Commit: {}", txid);
	}

	@Override
	public void commit(Long txid) {
		LOG.debug("End Commit: {}", txid);
	}
	
	@SuppressWarnings("unchecked")
	public void update(List<TridentTuple> tuples, TridentCollector collector){
		LOG.debug("updating with {} tuples", tuples.size());
		
		try {
			if(this.maxBatchSize > 0){
				int size = tuples.size();
				int batchCount = size / this.maxBatchSize;
				int count = 0;
				for (int i = 0;i < batchCount;i++){
					this.client.writeTuples(tuples.subList(i * this.maxBatchSize, (i + 1) * this.maxBatchSize), this.mapper);
					count++;
				}
				this.client.writeTuples(tuples.subList(count * this.maxBatchSize, size), this.mapper);
	
				for(TridentTuple tuple : tuples){
					collector.emit(tuple.getValues());
				}
			} else {
				this.client.writeTuples(tuples, this.mapper);
			}

		} catch (Exception e) {
			LOG.warn("Batch write failed. Triggering replay.", e);
			throw new FailedException(e);
		}
	}
	

}
