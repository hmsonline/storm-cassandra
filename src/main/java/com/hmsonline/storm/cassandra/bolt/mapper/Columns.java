package com.hmsonline.storm.cassandra.bolt.mapper;

import java.util.Iterator;

/**
 * This is the interface that shields clients from the underlying cassandra client in use by the bolts.
 * @author boneill42
 * @param <T> is the type of the column name. (NOT the value)
 */
public interface Columns<T> {
    public String getColumnValue(T columnName);
    
    public Iterator<T> getColumnNames();
}
