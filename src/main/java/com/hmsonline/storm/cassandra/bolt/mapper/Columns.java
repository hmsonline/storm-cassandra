package com.hmsonline.storm.cassandra.bolt.mapper;

/**
 * This is the interface that shields clients from the underlying cassandra
 * client in use by the bolts.
 * 
 * @author boneill42
 * @param <T>
 *            is the type of the column name. (NOT the value)
 */
public interface Columns<K, V> extends Iterable<Column<K, V>> {

    V getColumnByIndex(int i);

    V getColumnByName(K name);

    int size();
}
