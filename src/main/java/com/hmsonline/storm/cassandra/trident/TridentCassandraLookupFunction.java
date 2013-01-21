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

public class TridentCassandraLookupFunction<K, V> implements Function {
    private static final long serialVersionUID = 12132012L;

    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraLookupFunction.class);

    private TridentColumnMapper<K, V> columnsMapper;
    private TridentTupleMapper<K, V> tupleMapper;
    private CassandraClient<K, V> client;
    private Class<K> columnNameClass;
    private Class<V> columnValueClass;
    private String clientConfigKey;

    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, V> tupleMapper,
            TridentColumnMapper<K, V> columnMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
        this.clientConfigKey = clientConfigKey;
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
        Object start = tupleMapper.mapToStartKey(input);
        Object end = tupleMapper.mapToEndKey(input);

        try {
            Columns<K, V> colMap = null;
            if (start == null || end == null) {
                colMap = client.lookup(columnFamily, rowKey);
            } else {
                colMap = client.lookup(columnFamily, rowKey, start, end);
            }

            List<Values> valuesToEmit = columnsMapper.mapToValues(rowKey, colMap, input);
            for (Values values : valuesToEmit) {
                collector.emit(values);
            }

        } catch (Exception e) {
            LOG.warn("Could not emit for row [" + rowKey + "] from Cassandra." + " :" + e.getMessage(), e);
        }
    }
}
