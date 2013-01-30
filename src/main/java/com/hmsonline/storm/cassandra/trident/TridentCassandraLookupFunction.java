package com.hmsonline.storm.cassandra.trident;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
<<<<<<< HEAD
=======
import com.hmsonline.storm.cassandra.client.CassandraClient;
import java.util.ArrayList;
import storm.trident.operation.Filter;
>>>>>>> 4ebcb502076a03e359333b98df1b2b26ef62897b

public class TridentCassandraLookupFunction<K, C, V> implements Function {
    private static final long serialVersionUID = 12132012L;

    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraLookupFunction.class);

    private TridentColumnMapper<K, C, V> columnsMapper;
    private TridentTupleMapper<K, C, V> tupleMapper;
    private AstyanaxClient<K, C, V> client;
    private String clientConfigKey;

<<<<<<< HEAD
    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper,
            TridentColumnMapper<K, C, V> columnMapper) {
=======
    private Filter tupleFilter = null;      // used to prevent processing for tuples that should be skipped by the lookup
    private int numberOfOutputFields = 1;   // used to emit when the incoming tuple doesn't pass the filter check
    private boolean emitEmptyOnFailure = false;

    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, V> tupleMapper,
            TridentColumnMapper<K, V> columnMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
>>>>>>> 4ebcb502076a03e359333b98df1b2b26ef62897b
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
        this.clientConfigKey = clientConfigKey;
    }
    
    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, V> tupleMapper,
            TridentColumnMapper<K, V> columnMapper, Class<K> columnNameClass, Class<V> columnValueClass, boolean emitEmptyOnFailure) {
        this(clientConfigKey, tupleMapper, columnMapper, columnNameClass, columnValueClass);
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
    public void execute(TridentTuple input, TridentCollector collector) {
        String columnFamily = tupleMapper.mapToColumnFamily(input);
<<<<<<< HEAD
        K rowKey = tupleMapper.mapToRowKey(input);
        C start = tupleMapper.mapToStartKey(input);
        C end = tupleMapper.mapToEndKey(input);
=======
        String rowKey = tupleMapper.mapToRowKey(input);
        if (tupleFilter != null && !tupleFilter.isKeep(input)) {
            collector.emit(createEmptyValues());
            return;
        }
        Object start = tupleMapper.mapToStartKey(input);
        Object end = tupleMapper.mapToEndKey(input);
>>>>>>> 4ebcb502076a03e359333b98df1b2b26ef62897b

        try {
            Map<C, V> colMap = null;
            if (start == null || end == null) {
                colMap = client.lookup(tupleMapper, input);
            } else {
                colMap = client.lookup(tupleMapper, input, start, end);
            }

            List<Values> valuesToEmit = columnsMapper.mapToValues(rowKey, colMap, input);
            for (Values values : valuesToEmit) {
                collector.emit(values);
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
        //String[] emptyValues = new String[this.numberOfOutputFields];
        ArrayList<Object> emptyValues = new ArrayList<Object>();
        for (int evc=0;evc<this.numberOfOutputFields;evc++) {
            emptyValues.add("");
//            emptyValues[evc] = "";
        }
//        Values valuesToEmit = new Values((Object)emptyValues);
        Values valuesToEmit = new Values(emptyValues.toArray());
        return valuesToEmit;
    }
}
