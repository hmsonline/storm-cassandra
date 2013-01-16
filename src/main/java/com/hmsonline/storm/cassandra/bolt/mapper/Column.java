package com.hmsonline.storm.cassandra.bolt.mapper;

public interface Column<K,V> {
	public K getKey();
	public V getValue();
}
