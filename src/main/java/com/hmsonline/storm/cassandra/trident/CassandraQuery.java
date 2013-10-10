package com.hmsonline.storm.cassandra.trident;

import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;

import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class CassandraQuery extends BaseQueryFunction<CassandraState, Map<?,?>> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraQuery.class);
    
    private TridentColumnMapper columnsMapper;
    private TridentTupleMapper tupleMapper;
    private ExceptionHandler exceptionHandler;

    public CassandraQuery(TridentTupleMapper tupleMapper,
                          TridentColumnMapper columnMapper){
        this(tupleMapper, columnMapper, null);
    }

    public CassandraQuery(TridentTupleMapper tupleMapper,
            TridentColumnMapper columnMapper, ExceptionHandler exceptionHandler){
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public List<Map<?,?>> batchRetrieve(CassandraState state, List<TridentTuple> args) {
        return state.batchRetrieve(args, this.tupleMapper);
    }

    @Override
    public void execute(TridentTuple tuple, Map<?,?> result, TridentCollector collector) {
        try{
            List<Values> emitList = this.columnsMapper.mapToValues(this.tupleMapper.mapToRowKey(tuple), result, tuple);
            for(Values values : emitList){
                collector.emit(values);
            }
        } catch (Exception e){
            if(this.exceptionHandler != null){
                this.exceptionHandler.onException(e, collector);
            } else {
                LOG.warn("Error mapping columns to values.", e);
            }
        }
    }

}
