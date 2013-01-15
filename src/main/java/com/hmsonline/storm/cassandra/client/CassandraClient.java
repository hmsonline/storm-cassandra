package com.hmsonline.storm.cassandra.client;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

import backtype.storm.tuple.Tuple;

public abstract class CassandraClient<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private Class<T> columnNameClass;

    public void setColumnNameClass(Class<T> columnNameClass) {
        this.columnNameClass = columnNameClass;
    }

    protected Class<T> getColumnNameClass(){
        return this.columnNameClass;
    }

    public abstract void start(String cassandraHost, String cassandraKeyspace);

    public abstract void stop();

    public abstract Columns<T> lookup(String columnFamilyName, String rowKey) throws Exception;

    public abstract Columns<T> lookup(String columnFamilyName, String rowKey, List<T> columns) throws Exception;

    public abstract Columns<T> lookup(String columnFamilyName, String rowKey, Object start, Object end) throws Exception;

    public abstract void writeTuple(Tuple input, TupleMapper<T> tupleMapper) throws Exception;

    public abstract void writeTuples(List<Tuple> inputs, TupleMapper<T> tupleMapper) throws Exception;

	public abstract void incrementCountColumn(Tuple input,
			TupleCounterMapper tupleMapper) throws Exception;
    public abstract void writeTuple(TridentTuple input, TridentTupleMapper<T> tupleMapper) throws Exception;

	public abstract void incrementCountColumns(List<Tuple> inputs,
			TupleCounterMapper tupleMapper) throws Exception;
    public abstract void write(String columnFamilyName, String rowKey, Map<T, String> columns) ;

    public abstract String getClientKeySpace();

}