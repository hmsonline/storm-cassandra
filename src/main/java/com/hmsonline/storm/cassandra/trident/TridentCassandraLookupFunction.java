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
    private String cassandraHost;
    private String cassandraKeyspace;
    private TridentColumnMapper<K, V> columnsMapper;
    private TridentTupleMapper<K, V> tupleMapper;
    private CassandraClient<K, V> client;
    private Class<K> columnNameClass;
    private Class<V> columnValueClass;

    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";
    protected Map<String, Object> stormConfig;

    public TridentCassandraLookupFunction(TridentTupleMapper<K, V> tupleMapper, TridentColumnMapper<K, V> columnMapper,
            Class<K> columnNameClass, Class<V> columnValueClass) {
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void prepare(Map stormConf, TridentOperationContext context) {
        this.stormConfig = stormConf;
        if (this.cassandraHost == null) {
            this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        }
        if (this.cassandraKeyspace == null) {
            this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        }

        LOG.error("Creating new Cassandra Client @ (" + this.cassandraHost + ":" + this.cassandraKeyspace + ")");
        client = new AstyanaxClient<K, V>(columnNameClass, columnValueClass);
        client.start(this.cassandraHost, this.cassandraKeyspace, stormConfig);
    }

    @Override
    public void cleanup() {
        // TODO: Come back and fix this.

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
