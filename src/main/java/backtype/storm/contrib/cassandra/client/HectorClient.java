package backtype.storm.contrib.cassandra.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import backtype.storm.contrib.cassandra.bolt.mapper.TupleMapper;
import backtype.storm.tuple.Tuple;

public class HectorClient implements CassandraClient {

    private Cluster cluster;
    private Keyspace keyspace;

    @Override
    public void start(String cassandraHost, String cassandraKeyspace) {
        CassandraHostConfigurator chc = new CassandraHostConfigurator(cassandraHost);
        chc.setAutoDiscoverHosts(true);
        chc.setRunAutoDiscoveryAtStartup(false);
        this.cluster = HFactory.getOrCreateCluster("cirrus-cluster", chc);
        this.keyspace = HFactory.createKeyspace(cassandraKeyspace, this.cluster);

    }

    @Override
    public void stop() {
        this.cluster.getConnectionManager().shutdown();
    }

    @Override
    public Map<String, String> lookup(String columnFamilyName, String rowKey) throws Exception {
        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(this.keyspace,
                columnFamilyName, new StringSerializer(), new StringSerializer());
        ColumnFamilyResult<String, String> result = template.queryColumns(rowKey);
        HashMap<String, String> colMap = new HashMap<String, String>();
        Collection<String> cols = result.getColumnNames();
        for (String colName : cols) {
            colMap.put(colName, result.getString(colName));
        }
        return colMap;
    }

    @Override
    public void writeTuple(Tuple input, TupleMapper tupleMapper) throws Exception {
        String rowKey = (String) tupleMapper.mapToRowKey(input);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, new StringSerializer());
        addTupleToMutation(input, rowKey, mutator, tupleMapper);
        mutator.execute();
    }

    private void addTupleToMutation(Tuple input, String rowKey, Mutator<String> mutator, TupleMapper tupleMapper) {
        Map<String, String> columns = tupleMapper.mapToColumns(input);
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            mutator.addInsertion(rowKey, tupleMapper.mapToColumnFamily(input),
                    HFactory.createStringColumn(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void writeTuples(List<Tuple> inputs, TupleMapper tupleMapper) throws Exception {
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, new StringSerializer());
        for(Tuple input : inputs){
            String rowKey = (String) tupleMapper.mapToRowKey(input);
            this.addTupleToMutation(input, rowKey, mutator, tupleMapper);
        }
        mutator.execute();

    }

}
