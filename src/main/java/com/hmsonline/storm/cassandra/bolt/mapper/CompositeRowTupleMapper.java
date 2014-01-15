package com.hmsonline.storm.cassandra.bolt.mapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.StringSerializer;

public class CompositeRowTupleMapper implements TupleMapper<Composite, String, String> {
    private static final long serialVersionUID = 1L;
    private String[] rowKeyFields;
    private String columnFamily;
    private String keyspace;

    /**
     * This mapper is similar to the DefaultTupleMapper, but supports composite row keys
     * constructed from multiple fields.
     *
     * @param keyspace
     *            keyspace to write to.
     * @param columnFamily
     *            column family to write to.
     * @param rowKeyFields
     *            tuple fields to use as the composite row key.
     */
    public CompositeRowTupleMapper(String keyspace, String columnFamily, String... rowKeyFields) {
        this.rowKeyFields = rowKeyFields;
        this.columnFamily = columnFamily;
        this.keyspace = keyspace;
    }

    @Override
    public Composite mapToRowKey(Tuple tuple) {
        Composite keyName = new Composite();

        for (String rowKeyField : this.rowKeyFields){
            Object component = tuple.getValueByField(rowKeyField);
            if (component == null) {
                component = "[NULL]";
            }

            keyName.addComponent(component.toString(), StringSerializer.get());
        }

        return keyName;
    }

    @Override
    public String mapToKeyspace(Tuple tuple) {
        return this.keyspace;
    }

    /**
     * Write each value in the tuple as a key:value pair
     * in the Cassandra row, excluding fields that were included in the row.
     *
     * @param tuple
     * @return map of columns to values
     */
    @Override
    public Map<String, String> mapToColumns(Tuple tuple) {
        Fields fields = tuple.getFields();
        Map<String, String> columns = new HashMap<String, String>();
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i);
            Boolean isRowField = Arrays.asList(this.rowKeyFields).contains(name);
            if (!isRowField) {
                Object value = tuple.getValueByField(name);
                columns.put(name, (value != null ? value.toString() : ""));
            }
        }
        return columns;
    }

    @Override
    public String mapToColumnFamily(Tuple tuple) {
        return this.columnFamily;
    }

    @Override
    public Class<Composite> getKeyClass() {
        // TODO Auto-generated method stub
        return Composite.class;
    }

    @Override
    public Class<String> getColumnNameClass() {
        // TODO Auto-generated method stub
        return String.class;
    }

    @Override
    public Class<String> getColumnValueClass() {
        // TODO Auto-generated method stub
        return String.class;
    }


}
