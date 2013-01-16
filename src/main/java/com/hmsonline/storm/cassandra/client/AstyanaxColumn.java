package com.hmsonline.storm.cassandra.client;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;

public class AstyanaxColumn<K, V> implements com.hmsonline.storm.cassandra.bolt.mapper.Column<K, V> {
    private Column<K> column = null;
    private Serializer<V> serializer = null;

    public AstyanaxColumn(Column<K> column, Serializer<V> serializer) {
        this.column = column;
    }

    @Override
    public K getKey() {
        return column.getName();
    }

    @Override
    public V getValue() {
        return column.getValue(serializer);
    }
}
