package backtype.storm.contrib.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.mapper.TupleCounterMapper;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.contrib.cassandra.client.CassandraClient;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public abstract class CassandraBolt implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);
    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";
    
    private String cassandraHost;
    private String cassandraKeyspace;
    protected CassandraClient cassandraClient;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        initCassandraConnection(stormConf);
    }

    @SuppressWarnings("rawtypes")
    private void initCassandraConnection(Map conf) {
        try {
            String clazz = (String)conf.get(CASSANDRA_CLIENT_CLASS);
            if(clazz == null){
                clazz = "backtype.storm.contrib.cassandra.client.AstyanaxClient";
            }
            Class cl = Class.forName(clazz);
            this.cassandraClient = (CassandraClient)cl.newInstance();
            this.cassandraClient.start(this.cassandraHost, this.cassandraKeyspace);
        } catch (Throwable e) {
            LOG.warn("Preparation failed.", e);
            throw new IllegalStateException("Failed to prepare CassandraBolt", e);
        }
    }    

    public void cleanup(){
        this.cassandraClient.stop();
    }

    public void writeTuple(Tuple input, TupleMapper tupleMapper) throws Exception {
        this.cassandraClient.writeTuple(input, tupleMapper);
    }

    public void writeTuples(List<Tuple> inputs, TupleMapper tupleMapper) throws Exception {
        this.cassandraClient.writeTuples(inputs, tupleMapper);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    public void incrementCounter(Tuple input, TupleCounterMapper tupleMapper) throws Exception{
    	this.cassandraClient.incrementCountColumn(input, tupleMapper);
    }
    
    public void incrementCounters(List<Tuple> inputs, TupleCounterMapper tupleMapper) throws Exception{
    	this.cassandraClient.incrementCountColumns(inputs, tupleMapper);
    }
}
