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

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.CassandraClient;
import java.util.ArrayList;
import storm.trident.operation.Filter;

public class TridentCassandraLookupFunction<K, V> implements Function {
    private static final long serialVersionUID = 12132012L;

    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraLookupFunction.class);

    private TridentColumnMapper<K, V> columnsMapper;
    private TridentTupleMapper<K, V> tupleMapper;
    private CassandraClient<K, V> client;
    private Class<K> columnNameClass;
    private Class<V> columnValueClass;
    private String clientConfigKey;

    private Filter tupleFilter = null;      // used to prevent processing for tuples that should be skipped by the lookup
    private int numberOfOutputFields = 1;   // used to emit when the incoming tuple doesn't pass the filter check
    private boolean emitEmptyOnFailure = false;

    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, V> tupleMapper,
            TridentColumnMapper<K, V> columnMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
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

        this.client = new AstyanaxClient<K, V>(columnNameClass, columnValueClass);
        this.client.start(config);
    }

    @Override
    public void cleanup() {
        this.client.stop();
    }

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {
        String columnFamily = tupleMapper.mapToColumnFamily(input);
        String rowKey = tupleMapper.mapToRowKey(input);
        if (tupleFilter != null && !tupleFilter.isKeep(input)) {
            collector.emit(createEmptyValues());
            return;
        }
        Object start = tupleMapper.mapToStartKey(input);
        Object end = tupleMapper.mapToEndKey(input);

        List<Object> startList = tupleMapper.mapToStartKeyList(input);
        List<Object> endList = tupleMapper.mapToEndKeyList(input);

        try {
            List<Values> valuesToEmit;

            if (startList != null && endList != null && startList.size() == endList.size()) {
                List<Columns<K, V>> colMapList = new ArrayList<Columns<K, V>>();
                for (int i = 0; i < startList.size(); i++) {
                    Columns<K, V> colMap = client.lookup(columnFamily, rowKey, startList.get(i), endList.get(i));
                    if (colMap != null) {
                        colMapList.add(colMap);
                    }
                }
                valuesToEmit = columnsMapper.mapToValues(rowKey, colMapList, input);
            }
            else {
                Columns<K, V> colMap = null;

                if (start == null || end == null) {
                    colMap = client.lookup(columnFamily, rowKey);
                } else {
                    colMap = client.lookup(columnFamily, rowKey, start, end);
                }
                valuesToEmit = columnsMapper.mapToValues(rowKey, colMap, input);
            }
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
