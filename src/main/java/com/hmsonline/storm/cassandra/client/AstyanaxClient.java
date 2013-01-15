package com.hmsonline.storm.cassandra.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.cassandra.bolt.mapper.Columns;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * 
 * @author tgoetz
 * 
 */
public class AstyanaxClient<T> extends CassandraClient<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxClient.class);
    private AstyanaxContext<Keyspace> astyanaxContext;
    protected Cluster cluster;
    protected Keyspace keyspace;

    /*
     * (non-Javadoc)
     * 
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#start(java.lang
     * .String, java.lang.String)
     */
    @Override
    public void start(String cassandraHost, String cassandraKeyspace) {
        try {
            this.astyanaxContext = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(cassandraKeyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                    .withConnectionPoolConfiguration(
                            new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1).setSeeds(
                                    cassandraHost)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            this.astyanaxContext.start();
            this.keyspace = this.astyanaxContext.getEntity();
            // test the connection
            this.keyspace.describeKeyspace();
        } catch (Throwable e) {
            LOG.warn("Astyanax initialization failed.", e);
            throw new IllegalStateException("Failed to prepare Astyanax", e);
        }
    }

    public String getClientKeySpace(){
        return astyanaxContext.getKeyspaceName();
    }
    /*
     * (non-Javadoc)
     * 
     * @see backtype.storm.contrib.cassandra.client.CassandraClient#stop()
     */
    @Override
    public void stop() {
        this.astyanaxContext.shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#lookup(java.lang
     * .String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public Columns<T> lookup(String columnFamilyName, String rowKey) throws Exception {
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                StringSerializer.get(), getColumnNameSerializer());
        OperationResult<ColumnList<T>> result;
        result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();
        ColumnList<T> columns = (ColumnList<T>) result.getResult();
        return new AstyanaxColumns<T>(columns);
    }

    @Override
    public Columns<T> lookup(String columnFamilyName, String rowKey, List<T> columns) throws Exception {
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                StringSerializer.get(), getColumnNameSerializer());
        OperationResult<ColumnList<T>> result;
        result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey)
                .withColumnSlice((Collection) columns).execute();
        ColumnList<T> resultColumns = (ColumnList<T>) result.getResult();
        return new AstyanaxColumns<T>(resultColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#lookup(java.lang
     * .String, java.lang.String)
     */
    @Override
    public Columns<T> lookup(String columnFamilyName, String rowKey, Object start, Object end) throws Exception {
        if(start == null || end == null){
            return null;
        }
        Serializer<T> serializer = getColumnNameSerializer();
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                StringSerializer.get(), serializer);
        OperationResult<ColumnList<T>> result;
        result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey)
                 .withColumnRange(getRangeBuilder(start, end, serializer))
                 .execute();
        ColumnList<T> columns = (ColumnList<T>) result.getResult();
        return new AstyanaxColumns<T>(columns);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#writeTuple(backtype
     * .storm.tuple.Tuple,
     * backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuple(Tuple input, TupleMapper<T> tupleMapper) throws Exception {
        String columnFamilyName = tupleMapper.mapToColumnFamily(input);
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName, StringSerializer.get(),
                this.getColumnNameSerializer(tupleMapper));
        this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        mutation.execute();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#writeTuple(backtype
     * .storm.tuple.Tuple,
     * backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuple(TridentTuple input, TridentTupleMapper<T> tupleMapper) throws Exception {
        String columnFamilyName = tupleMapper.mapToColumnFamily(input);
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName, StringSerializer.get(),
                this.getColumnNameSerializer());
        this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        mutation.execute();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.contrib.cassandra.client.CassandraClient#writeTuples(java
     * .util.List, backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper)
     */
    @Override
    public void writeTuples(List<Tuple> inputs, TupleMapper<T> tupleMapper) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        for (Tuple input : inputs) {
            String columnFamilyName = tupleMapper.mapToColumnFamily(input);
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                    StringSerializer.get(), this.getColumnNameSerializer(tupleMapper));
            this.addTupleToMutation(input, columnFamily, rowKey, mutation, tupleMapper);
        }
        mutation.execute();
    }

    /**
     * Writes columns.
     */
    public void write(String columnFamilyName, String rowKey, Map<T, String> columns) {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, T> columnFamily = new ColumnFamily<String, T>(columnFamilyName,
                StringSerializer.get(), this.getColumnNameSerializer());
        for (Map.Entry<T, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
        try {
            mutation.execute();
        } catch (Exception e) {
            LOG.error("Could not execute mutation.", e);
        }
    }

    private void addTupleToMutation(Tuple input, ColumnFamily<String, T> columnFamily, String rowKey,
            MutationBatch mutation, TupleMapper<T> tupleMapper) {
        Map<T, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<T, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
    }

    private void addTupleToMutation(TridentTuple input, ColumnFamily<String, T> columnFamily, String rowKey,
            MutationBatch mutation, TridentTupleMapper<T> tupleMapper) {
        Map<T, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<T, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
    }

    @SuppressWarnings("unchecked")
    private Serializer<T> getColumnNameSerializer(TupleMapper<T> tupleMapper) {
        if (this.getColumnNameClass().equals(String.class)) {
            return (Serializer<T>) StringSerializer.get();
        } else {
            // TODO: Cache this instance.
            return new AnnotatedCompositeSerializer<T>(this.getColumnNameClass());
        }
    }

    @Override
    public void incrementCountColumn(Tuple input, TupleCounterMapper tupleMapper) throws Exception {
            String columnFamilyName = tupleMapper.mapToColumnFamily(input);
    String rowKey = (String) tupleMapper.mapToRowKey(input);
    long incrementAmount = tupleMapper.mapToIncrementAmount(input);
    MutationBatch mutation = keyspace.prepareMutationBatch();
    ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                    StringSerializer.get(), StringSerializer.get());
    for(String columnName : tupleMapper.mapToColumnList(input)){
            mutation.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, incrementAmount);
    }
    mutation.execute();
    }

    @Override
    public void incrementCountColumns(List<Tuple> inputs,
                    TupleCounterMapper tupleMapper) throws Exception {
            MutationBatch mutation = keyspace.prepareMutationBatch();
            for (Tuple input : inputs) {
                    String columnFamilyName = tupleMapper.mapToColumnFamily(input);
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            long incrementAmount = tupleMapper.mapToIncrementAmount(input);
            ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                    StringSerializer.get(), StringSerializer.get());
            for(String columnName : tupleMapper.mapToColumnList(input)){
                    mutation.withRow(columnFamily, rowKey).incrementCounterColumn(columnName, incrementAmount);
            }
            }
            mutation.execute();
    }
    
    
    @SuppressWarnings("unchecked")
    private Serializer<T> getColumnNameSerializer() {
        if (this.getColumnNameClass().equals(String.class)) {
            return (Serializer<T>) StringSerializer.get();
        } else {
            return new AnnotatedCompositeSerializer<T>(this.getColumnNameClass());
        }
    }

    private ByteBufferRange getRangeBuilder(Object start, Object end, Serializer<T> serializer) {
        if (this.getColumnNameClass().equals(String.class)) {
            return new RangeBuilder().setStart((String) start).setEnd((String) end).build();
        } else {
            return ((AnnotatedCompositeSerializer<T>) serializer).buildRange().greaterThanEquals(start)
                    .lessThanEquals(end).build();
        }
    }

}
