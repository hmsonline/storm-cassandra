package com.hmsonline.storm.cassandra.trident;

import java.util.List;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class CassandraUpdater<K, C, V> extends BaseStateUpdater<CassandraState> {

    private static final long serialVersionUID = 1115563296010140546L;
    
    private TridentTupleMapper<K, C, V> tupleMapper;
    
    public CassandraUpdater(TridentTupleMapper<K, C, V> tupleMapper){
        this.tupleMapper = tupleMapper;
    }

    @Override
    public void updateState(CassandraState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.update(tuples, collector, this.tupleMapper);
    }

}
