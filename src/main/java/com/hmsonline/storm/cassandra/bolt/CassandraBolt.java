package com.hmsonline.storm.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.CassandraClient;

@SuppressWarnings("serial")
public abstract class CassandraBolt<K, V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);

    private String cassandraHost;
    private String cassandraKeyspace;
    private Class<K> columnNameClass;
    private Class<V> columnValueClass;
    private CassandraClient<K, V> client;

    protected TupleMapper<K, V> tupleMapper;
    protected CassandraClient<K, V> cassandraClient;
    protected Map<String, Object> stormConfig;

    public CassandraBolt(TupleMapper<K, V> tupleMapper, Class<K> columnNameClass, Class<V> columnValueClass) {
        this.tupleMapper = tupleMapper;
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;

        LOG.debug("Creating Cassandra Bolt (" + this + ")");
    }

    public void prepare(Map<String, Object> stormConf, TopologyContext context) {
        this.stormConfig = stormConf;
        if (this.cassandraHost == null) {
            this.cassandraHost = (String) stormConf.get(StormCassandraConstants.CASSANDRA_HOST);
        }
        if (this.cassandraKeyspace == null) {
            this.cassandraKeyspace = (String) stormConf.get(StormCassandraConstants.CASSANDRA_KEYSPACE);
        }
        LOG.error("Creating new Cassandra Client @ (" + this.cassandraHost + ":" + this.cassandraKeyspace + ")");
        client = new AstyanaxClient<K, V>(columnNameClass, columnValueClass);
        client.start(this.cassandraHost, this.cassandraKeyspace, stormConfig);
    }

    public CassandraClient<K, V> getClient() {
        return client;
    }

    public void cleanup() {
        // No longer stop the client since it might be shared.
        // TODO: Come back and fix this.
        // getClient().stop();
    }

    public void writeTuple(Tuple input, TupleMapper<K, V> tupleMapper) throws Exception {
        getClient().writeTuple(input, tupleMapper);
    }

    public void writeTuples(List<Tuple> inputs, TupleMapper<K, V> tupleMapper) throws Exception {
        getClient().writeTuples(inputs, tupleMapper);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void incrementCounter(Tuple input, TupleCounterMapper tupleMapper) throws Exception {
        getClient().incrementCountColumn(input, tupleMapper);
    }

    public void incrementCounters(List<Tuple> inputs, TupleCounterMapper tupleMapper) throws Exception {
        getClient().incrementCountColumns(inputs, tupleMapper);
    }
}
