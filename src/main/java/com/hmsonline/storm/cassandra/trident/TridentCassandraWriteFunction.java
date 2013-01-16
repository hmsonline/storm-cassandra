package com.hmsonline.storm.cassandra.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.CassandraClient;

public class TridentCassandraWriteFunction<K,V> implements Function{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(TridentCassandraWriteFunction.class);
    protected TridentTupleMapper<K,V> tupleMapper;

    public static String CASSANDRA_HOST = "cassandra.host";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCH_MAX_SIZE = "cassandra.batch.max_size";
    public static String CASSANDRA_CLIENT_CLASS = "cassandra.client.class";

    private String cassandraHost;
    private String cassandraKeyspace;
    protected Map<String, Object> stormConfig;
    private CassandraClient<K,V> client;
    private Class<K> columnNameClass;
    private Class<V> columnValueClass;

    public TridentCassandraWriteFunction(TridentTupleMapper<K,V> tupleMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        this.tupleMapper = tupleMapper;
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
    }

	@Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void prepare(Map stormConf, TridentOperationContext context) {
        this.stormConfig = stormConf;
        if(this.cassandraHost == null){
            this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        }
        if (this.cassandraKeyspace == null){
            this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        }

        LOG.error("Creating new Cassandra Client @ (" + this.cassandraHost + ":" + this.cassandraKeyspace + ")");
        client = new AstyanaxClient<K,V>(columnNameClass, columnValueClass);
        client.start(this.cassandraHost, this.cassandraKeyspace, stormConfig);
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

    public synchronized CassandraClient<K,V> getClient(){
    	if (client == null){
    		
    	}
    	return client;
    }
}
