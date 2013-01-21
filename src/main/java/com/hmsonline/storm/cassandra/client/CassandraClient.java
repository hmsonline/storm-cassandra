package com.hmsonline.storm.cassandra.client;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

public abstract class CassandraClient<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;
    private Class<K> columnNameClass;
    private Class<V> columnValueClass;

    public CassandraClient(Class<K> columnNameClass, Class<V> columnValueClass) {
        this.columnNameClass = columnNameClass;
        this.columnValueClass = columnValueClass;
    }

    protected Class<K> getColumnNameClass() {
        return this.columnNameClass;
    }

    protected Class<V> getColumnValueClass() {
        return this.columnValueClass;
    }

    public abstract void start(Map<String, Object> stormConfig);

    public abstract void stop();

    public abstract Columns<K, V> lookup(String columnFamilyName, String rowKey) throws Exception;

    public abstract Columns<K, V> lookup(String columnFamilyName, String rowKey, List<K> columns) throws Exception;

    public abstract Columns<K, V> lookup(String columnFamilyName, String rowKey, Object start, Object end)
            throws Exception;

    public abstract void writeTuple(Tuple input, TupleMapper<K, V> tupleMapper) throws Exception;

    public abstract void writeTuples(List<Tuple> inputs, TupleMapper<K, V> tupleMapper) throws Exception;

    public abstract void incrementCountColumn(Tuple input, TupleCounterMapper tupleMapper) throws Exception;

    public abstract void writeTuple(TridentTuple input, TridentTupleMapper<K, V> tupleMapper) throws Exception;

    public abstract void incrementCountColumns(List<Tuple> inputs, TupleCounterMapper tupleMapper) throws Exception;

    public abstract void write(String columnFamilyName, String rowKey, Map<K, String> columns);

    public abstract String getClientKeySpace();

}