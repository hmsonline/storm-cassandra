package com.hmsonline.storm.cassandra.trident;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.ColumnsMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentColumnMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.CassandraClient;
import com.hmsonline.storm.cassandra.client.ClientPool;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;


public class TridentCassandraLookupFunction<T> implements Function{
    private static final long serialVersionUID = 12132012L;

    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraLookupFunction.class);
    private String cassandraHost;
    private String cassandraKeyspace;
    private Class columnNameClass;
    private String clientClass;
    private TridentColumnMapper<T> columnsMapper;
    private TridentTupleMapper<T> tupleMapper;

    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";

    public TridentCassandraLookupFunction(TridentTupleMapper<T> tupleMapper, TridentColumnMapper<T> columnMapper, Class columnNameClass ){
        this.columnNameClass = columnNameClass;
        this.columnsMapper = columnMapper;
        this.tupleMapper = tupleMapper;
    }

    @Override
    public void prepare(Map stormConf, TridentOperationContext context) {
        if(this.cassandraHost == null){
            this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        }
        if (this.cassandraKeyspace == null){
            this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        }

        if (this.clientClass == null){
            this.clientClass = (String) stormConf.get(CASSANDRA_CLIENT_CLASS);
        }
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
            Columns<T> colMap = null;
            if (start == null || end == null){
                colMap = getClient().lookup(columnFamily, rowKey);
            }else{
                colMap = getClient().lookup(columnFamily, rowKey, start, end);
            }


            List<Values> valuesToEmit = columnsMapper.mapToValues(rowKey, colMap, input);
            for (Values values : valuesToEmit) {
                collector.emit(values);
            }

        } catch (Exception e) {
            LOG.warn("Could not emit for row [" + rowKey + "] from Cassandra."+" :"+e.getMessage(), e);
        }

    }

    public CassandraClient<T> getClient(){
        return ClientPool.getClient(this.cassandraHost, this.cassandraKeyspace, this.columnNameClass, this.clientClass);
    }


}
