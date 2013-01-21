package com.hmsonline.storm.cassandra.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.CassandraClient;

@SuppressWarnings("serial")
public abstract class CassandraBolt<K, V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBolt.class);

    private Class<K> columnNameClass;
    private Class<V> columnValueClass;
    private String clientConfigKey;

    protected CassandraClient<K, V> client;

    protected TupleMapper<K, V> tupleMapper;
    protected Map<String, Object> stormConfig;

    public CassandraBolt(String clientConfigKey, TupleMapper<K, V> tupleMapper, Class<K> columnNameClass,
            Class<V> columnValueClass) {
        this.tupleMapper = tupleMapper;
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
        this.clientConfigKey = clientConfigKey;

        LOG.debug("Creating Cassandra Bolt (" + this + ")");
    }

    @SuppressWarnings("unchecked")
    public void prepare(Map<String, Object> stormConf, TopologyContext context) {
        Map<String, Object> config = (Map<String, Object>) stormConf.get(this.clientConfigKey);
        this.client = new AstyanaxClient<K, V>(columnNameClass, columnValueClass);
        this.client.start(config);
    }

    public void cleanup() {
        this.client.stop();
    }

    public void writeTuple(Tuple input, TupleMapper<K, V> tupleMapper) throws Exception {
        this.client.writeTuple(input, tupleMapper);
    }

    public void writeTuples(List<Tuple> inputs, TupleMapper<K, V> tupleMapper) throws Exception {
        this.client.writeTuples(inputs, tupleMapper);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void incrementCounter(Tuple input, TupleCounterMapper tupleMapper) throws Exception {
        this.client.incrementCountColumn(input, tupleMapper);
    }

    public void incrementCounters(List<Tuple> inputs, TupleCounterMapper tupleMapper) throws Exception {
        this.client.incrementCountColumns(inputs, tupleMapper);
    }
}
