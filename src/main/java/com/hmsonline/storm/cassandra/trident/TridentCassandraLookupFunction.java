package com.hmsonline.storm.cassandra.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.Equality;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;

public class TridentCassandraLookupFunction<K, C, V> implements Function {
    private static final long serialVersionUID = 12132012L;

    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraLookupFunction.class);

    private TridentColumnMapper<K, C, V> columnsMapper;
    private TridentTupleMapper<K, C, V> tupleMapper;
    private AstyanaxClient<K, C, V> client;
    private String clientConfigKey;

    private Filter tupleFilter = null;      // used to prevent processing for tuples that should be skipped by the lookup
    private int numberOfOutputFields = 1;   // used to emit when the incoming tuple doesn't pass the filter check
    private boolean emitEmptyOnFailure = false;


    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper,
            TridentColumnMapper<K, C, V> columnMapper) {
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
        this.clientConfigKey = clientConfigKey;
    }

    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper,
            TridentColumnMapper<K, C, V> columnMapper, boolean emitEmptyOnFailure) {
        this(clientConfigKey, tupleMapper, columnMapper);
        this.emitEmptyOnFailure = emitEmptyOnFailure;
    }

    public void setFilter(Filter filter) {
        this.tupleFilter = filter;
    }
    public void setNumberOfOutputFields(int numberOfFields) {
        this.numberOfOutputFields = numberOfFields;
    }
    public void setEmitEmptyOnFailure(boolean emitEmptyOnFailure) {
        this.emitEmptyOnFailure = emitEmptyOnFailure;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void prepare(Map stormConf, TridentOperationContext context) {
        Map<String, Object> config = (Map<String, Object>) stormConf.get(this.clientConfigKey);

        this.client = new AstyanaxClient<K, C, V>();
        this.client.start(config);
    }

    @Override
    public void cleanup() {
        this.client.stop();
    }

    @Override
    // TODO come back and fix this once composite range queries are sorted out
    // we should not have to execute multiple queries.
    public void execute(TridentTuple input, TridentCollector collector) {
        if (tupleFilter != null && !tupleFilter.isKeep(input)) {
            collector.emit(createEmptyValues());
            return;
        }

        K rowKey = tupleMapper.mapToRowKey(input);
        C start = tupleMapper.mapToStartKey(input);
        C end = tupleMapper.mapToEndKey(input);


        try {
            List<Values> valuesToEmit;
            Map<C, V> colMap = null;
            if (start == null || end == null) {
                colMap = client.lookup(tupleMapper, input);
            } else {
                colMap = client.lookup(tupleMapper, input, start, end, Equality.GREATER_THAN_EQUAL);
            }

            valuesToEmit = columnsMapper.mapToValues(rowKey, colMap, input);
            if(valuesToEmit != null){
                for (Values values : valuesToEmit) {
                    collector.emit(values);
                }
            }

        } catch (Exception e) {
            if (this.emitEmptyOnFailure) {
                LOG.info("Error processing tuple and will be emitting empty values.");
                collector.emit(createEmptyValues());
            }
            LOG.warn("Could not emit for row [" + rowKey + "] from Cassandra." + " :" + e.getMessage(), e);
        }
    }

    private Values createEmptyValues() {
        ArrayList<Object> emptyValues = new ArrayList<Object>();
        for (int evc=0;evc<this.numberOfOutputFields;evc++) {
            emptyValues.add("");
        }
        return new Values(emptyValues.toArray());
    }
}
