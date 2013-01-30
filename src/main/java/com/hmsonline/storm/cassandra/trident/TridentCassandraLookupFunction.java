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

public class TridentCassandraLookupFunction<K, C, V> implements Function {
    private static final long serialVersionUID = 12132012L;

    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraLookupFunction.class);

    private TridentColumnMapper<K, C, V> columnsMapper;
    private TridentTupleMapper<K, C, V> tupleMapper;
    private AstyanaxClient<K, C, V> client;
    private String clientConfigKey;

    public TridentCassandraLookupFunction(String clientConfigKey, TridentTupleMapper<K, C, V> tupleMapper,
            TridentColumnMapper<K, C, V> columnMapper) {
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
        this.clientConfigKey = clientConfigKey;
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
        K rowKey = tupleMapper.mapToRowKey(input);
        C start = tupleMapper.mapToStartKey(input);
        C end = tupleMapper.mapToEndKey(input);

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
            LOG.warn("Could not emit for row [" + rowKey + "] from Cassandra." + " :" + e.getMessage(), e);
        }
    }
}
