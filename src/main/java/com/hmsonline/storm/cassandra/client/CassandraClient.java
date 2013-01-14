package com.hmsonline.storm.cassandra.client;

import java.util.List;
import java.util.Map;

import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

import backtype.storm.tuple.Tuple;

public interface CassandraClient {

    public abstract void start(String cassandraHost, String cassandraKeyspace);

    public abstract void stop();

    public abstract Map<String, String> lookup(String columnFamilyName, String rowKey) throws Exception;

    public abstract void writeTuple(Tuple input, TupleMapper tupleMapper) throws Exception;

    public abstract void writeTuples(List<Tuple> inputs, TupleMapper tupleMapper) throws Exception;

	public abstract void incrementCountColumn(Tuple input,
			TupleCounterMapper tupleMapper) throws Exception;

	public abstract void incrementCountColumns(List<Tuple> inputs,
			TupleCounterMapper tupleMapper) throws Exception;

}