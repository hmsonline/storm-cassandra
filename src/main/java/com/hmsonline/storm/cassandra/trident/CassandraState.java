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
	@SuppressWarnings("rawtypes")
	private AstyanaxClient client = null;
	@SuppressWarnings("rawtypes")
	private TridentTupleMapper mapper = null;
	
	public CassandraState(AstyanaxClient<?, ?, ?> client, TridentTupleMapper<?, ?, ?> mapper){
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
			this.client.writeTuples(tuples, this.mapper);
			for(TridentTuple tuple : tuples){
				collector.emit(tuple.getValues());
			}

		} catch (Exception e) {
			LOG.warn("Batch write failed. Triggering replay.", e);
			throw new FailedException(e);
		}
	}
	

}
