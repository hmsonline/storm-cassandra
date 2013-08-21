package com.hmsonline.storm.cassandra.trident;

import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class CassandraQuery extends BaseQueryFunction<CassandraState, Map<?,?>> {
    
    private TridentColumnMapper columnsMapper;
    private TridentTupleMapper tupleMapper;
    
    
    public CassandraQuery(String clientConfigKey, TridentTupleMapper tupleMapper,
            TridentColumnMapper columnMapper){
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
    }

    @Override
    public List<Map<?,?>> batchRetrieve(CassandraState state, List<TridentTuple> args) {
        return state.batchRetrieve(args, this.tupleMapper);
    }

    @Override
    public void execute(TridentTuple tuple, Map<?,?> result, TridentCollector collector) {
        List<Values> emitList = this.columnsMapper.mapToValues(this.tupleMapper.mapToRowKey(tuple), result, tuple);
        for(Values values : emitList){
            collector.emit(values);
        }
    }

}
