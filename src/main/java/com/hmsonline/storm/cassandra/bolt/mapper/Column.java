package com.hmsonline.storm.cassandra.bolt.mapper;

public interface Column<K, V> {
    K getKey();

    V getValue();
}
