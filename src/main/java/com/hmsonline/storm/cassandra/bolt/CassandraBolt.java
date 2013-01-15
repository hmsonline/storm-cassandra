package com.hmsonline.storm.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.CassandraClient;
import com.hmsonline.storm.cassandra.client.ClientPool;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public abstract class CassandraBolt<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);
    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";
    
    private String cassandraHost;
    private String cassandraKeyspace;
    private Class columnNameClass;
    private String clientClass;
    protected TupleMapper<T> tupleMapper;
    protected CassandraClient<T> cassandraClient;
    protected Map<String, Object> stormConfig;
   

    public CassandraBolt(TupleMapper<T> tupleMapper, Class columnNameClass) {        
        this.columnNameClass = columnNameClass;
        this.tupleMapper = tupleMapper;
        LOG.debug("Creating Cassandra Bolt (" + this + ")");
    }

    public CassandraBolt(TupleMapper<T> tupleMapper) {
        this(tupleMapper, String.class);
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map<String, Object> stormConf, TopologyContext context) {
        this.stormConfig = stormConf;
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

    public CassandraClient<T> getClient(){
        return ClientPool.getClient(this.cassandraHost, this.cassandraKeyspace, this.columnNameClass, this.clientClass, this.stormConfig);
    }

    public void cleanup() {
        // No longer stop the client since it might be shared.
        // TODO: Come back and fix this.
        // getClient().stop();
    }

    public void writeTuple(Tuple input, TupleMapper<T> tupleMapper) throws Exception {
        getClient().writeTuple(input, tupleMapper);
    }

    public void writeTuples(List<Tuple> inputs, TupleMapper<T> tupleMapper) throws Exception {
        getClient().writeTuples(inputs, tupleMapper);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    public void incrementCounter(Tuple input, TupleCounterMapper tupleMapper) throws Exception{
        getClient().incrementCountColumn(input, tupleMapper);
    }
    
    public void incrementCounters(List<Tuple> inputs, TupleCounterMapper tupleMapper) throws Exception{
        getClient().incrementCountColumns(inputs, tupleMapper);
    }
}
