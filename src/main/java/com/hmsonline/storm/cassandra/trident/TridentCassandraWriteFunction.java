package com.hmsonline.storm.cassandra.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.CassandraClient;
import com.hmsonline.storm.cassandra.client.ClientPool;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TridentCassandraWriteFunction<T> implements Function{
    private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraWriteFunction.class);

    private Class columnNameClass;
    private String clientClass;
    protected TridentTupleMapper<T> tupleMapper;

    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";

    private String cassandraHost;
    private String cassandraKeyspace;

    public TridentCassandraWriteFunction(TridentTupleMapper<T> tupleMapper, Class columnNameClass) {
        this.tupleMapper = tupleMapper;
        this.columnNameClass = columnNameClass;
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
        // TODO Auto-generated method stub

    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            writeTuple(tuple);
        } catch (Exception e) {
            LOG.error("Failed to write tuple. Exception: " + e.getLocalizedMessage());
        }
    }

    public void writeTuple(TridentTuple input) throws Exception {
        getClient().writeTuple(input, this.tupleMapper);
    }

    public CassandraClient<T> getClient(){
        return ClientPool.getClient(this.cassandraHost, this.cassandraKeyspace, this.columnNameClass, this.clientClass);
    }
}
