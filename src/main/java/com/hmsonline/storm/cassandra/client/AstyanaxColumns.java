package com.hmsonline.storm.cassandra.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.hmsonline.storm.cassandra.bolt.mapper.Column;
import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnList;

public class AstyanaxColumns<K, V> implements Columns<K, V> {
    private ColumnList<K> astyanaxColumns;
    private Serializer<V> serializer;

    public AstyanaxColumns(ColumnList<K> columns, Serializer<V> serializer) {
        this.astyanaxColumns = columns;
        this.serializer = serializer;
    }

    @Override
    public V getColumnByName(K columnName) {
        return (V) astyanaxColumns.getColumnByName(columnName).getValue(serializer);
    }

    @Override
    public V getColumnByIndex(int i) {
        return (V) astyanaxColumns.getColumnByIndex(i).getValue(serializer);
    }

    @Override
    public int size() {
        return astyanaxColumns.size();
    }

    @Override
    public Iterator<Column<K, V>> iterator() {
        List<Column<K, V>> columns = new ArrayList<Column<K, V>>();
        for (com.netflix.astyanax.model.Column<K> astyanaxColumn : this.astyanaxColumns) {
            columns.add(new AstyanaxColumn<K, V>(astyanaxColumn, serializer));
        }
        return columns.iterator();
    }

}
