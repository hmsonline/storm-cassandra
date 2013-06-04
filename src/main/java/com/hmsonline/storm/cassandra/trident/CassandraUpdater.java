package com.hmsonline.storm.cassandra.trident;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class CassandraUpdater extends BaseStateUpdater<CassandraState> {

	private static final long serialVersionUID = 1115563296010140546L;

	@Override
	public void updateState(CassandraState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		state.update(tuples, collector);
	}

}
