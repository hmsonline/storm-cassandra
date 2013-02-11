package com.hmsonline.storm.cassandra.bolt;

import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.createColumnFamily;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newClusterContext;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newContext;
import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.Equality;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.composite.Composite2;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CompositeTest {
    private static Logger LOG = LoggerFactory.getLogger(CompositeTest.class);
    private static String KEYSPACE = CompositeTest.class.getSimpleName().toLowerCase();

    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        SingletonEmbeddedCassandra.getInstance();
        try {

            AstyanaxContext<Cluster> clusterContext = newClusterContext("localhost:9160");

            createColumnFamily(clusterContext, KEYSPACE, "simple", "UTF8Type", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "trident", "UTF8Type", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "generic", "UTF8Type", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "composite", "CompositeType(UTF8Type, UTF8Type)", "UTF8Type", "UTF8Type");

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testCompositeKey() throws Exception {
        TupleMapper tupleMapper = new SimpleTupleMapper("simple");
        AstyanaxClient client = new AstyanaxClient();
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, KEYSPACE);
        client.start(clientConfig);

        Fields fields = new Fields("key1", "key2", "foo", "bar");
        Values values = new Values("key1val", "key2val", "fooval", "barval");
        Tuple tuple = new MockTuple(fields, values);

        client.writeTuple(tuple, tupleMapper);

        AstyanaxContext<Keyspace> context = newContext("localhost:9160", KEYSPACE);
        Keyspace ks = context.getEntity();

        Column<String> result = ks
                .prepareQuery(
                        new ColumnFamily<SimpleComposite, String>("simple",
                                new AnnotatedCompositeSerializer<SimpleComposite>(SimpleComposite.class),
                                StringSerializer.get())).getKey(new SimpleComposite("key1val", "key2val"))
                .getColumn("foo").execute().getResult();
        assertEquals("fooval", result.getStringValue());
        
        // test lookup
        Map<String, String> map = client.lookup(tupleMapper, tuple);
        assertEquals("fooval", map.get("foo"));
        
        
        map = client.lookup(tupleMapper, tuple, "bar", "foo", Equality.EQUAL);
        assertNotNull(map.get("foo"));
        assertNotNull(map.get("bar"));
        assertNull(map.get("key1"));
        
        ArrayList<String> slice = new ArrayList<String>();
        slice.add("foo");
        slice.add("bar");
        slice.add("key1");
        map = client.lookup(tupleMapper, tuple, slice);
        assertNotNull(map.get("foo"));
        assertNotNull(map.get("bar"));
        assertNotNull(map.get("key1"));
        
        client.stop();

    }
    
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testTrident() throws Exception {
        AstyanaxClient client = new AstyanaxClient();
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, KEYSPACE);
        client.start(clientConfig);
        
        Fields fields = new Fields("key1", "key2", "foo", "bar");
        Values values = new Values("key1val", "key2val", "fooval", "barval");
        TridentTuple tuple = newTridentTuple(fields, values);
        
        SimpleTridentTupleMapper tupleMapper = new SimpleTridentTupleMapper(fields);
        
        client.writeTuple(tuple, tupleMapper);
        
        Map<String, String> map = client.lookup(tupleMapper, tuple);
        
        assertEquals("fooval", map.get("foo"));
        
        map = client.lookup(tupleMapper, tuple,"bar", "foo",Equality.GREATER_THAN_EQUAL);
        assertEquals("fooval", map.get("foo"));
        assertEquals("barval", map.get("bar"));
        assertNull(map.get("key1"));
        client.stop();
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testCompositeRangeQuery() throws InterruptedException {
        try{
        AstyanaxClient client = new AstyanaxClient();
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, KEYSPACE);
        client.start(clientConfig);
        
        Fields fields = new Fields("rowkey", "a", "b", "value");
        Values values = new Values("my_row", "a", "a", "aa");
        TridentTuple tuple = newTridentTuple(fields, values);
        
        CompositeColumnTridentTupleMapper tupleMapper = new CompositeColumnTridentTupleMapper();
        
        client.writeTuple(tuple, tupleMapper);
        
        tuple = newTridentTuple(fields, new Values("my_row", "b", "b", "bb"));
        client.writeTuple(tuple, tupleMapper);
        
        tuple = newTridentTuple(fields, new Values("my_row", "a", "b", "ab"));
        client.writeTuple(tuple, tupleMapper);
        
        tuple = newTridentTuple(fields, new Values("my_row", "a", "c", "ac"));
        client.writeTuple(tuple, tupleMapper);
        
        tuple = newTridentTuple(fields, new Values("my_row", "a", "d", "ad"));
        client.writeTuple(tuple, tupleMapper);
        
        tuple = newTridentTuple(fields, new Values("my_row", "c", "c", "cc"));
        client.writeTuple(tuple, tupleMapper);
        
        tuple = newTridentTuple(fields, new Values("my_row", "d", "d", "dd"));
        client.writeTuple(tuple, tupleMapper);
        
        
        Map<SimpleComposite, String> map = client.lookup(tupleMapper, tuple, new SimpleComposite("a", "a"), new SimpleComposite("a", "c"), Equality.GREATER_THAN_EQUAL);
        dumpMap(map);
        
        assertNotNull(map.get(new SimpleComposite("a", "a")));
        assertNotNull(map.get(new SimpleComposite("a", "b")));
        assertNotNull(map.get(new SimpleComposite("a", "c")));
        assertNull(map.get(new SimpleComposite("a", "d")));
        assertNull(map.get(new SimpleComposite("c", "c")));
        assertNull(map.get(new SimpleComposite("d", "d")));
        client.stop();
        } catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }
    
    //@Test
    // TODO this test depends on https://github.com/Netflix/astyanax/pull/203
    @SuppressWarnings("rawtypes")
    public void testGenericComposite() throws Exception {
        Composite2<String, String> c2 = new Composite2<String, String>("foo", "bar");
        LOG.warn("A class: {}", c2.a.getClass());
        
        
        Class<?> clazz = c2.getClass();
        Field[] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            LOG.warn("Field type: " + field.getType());
        }
        
        
        
        AstyanaxContext<Keyspace> context = newContext("localhost:9160", KEYSPACE);
        Keyspace ks = context.getEntity();
        
        AnnotatedCompositeSerializer<Composite2> acs = new AnnotatedCompositeSerializer<Composite2>(Composite2.class);
        
        ColumnFamily<String, Composite2> cf = new ColumnFamily<String, Composite2>("generic", StringSerializer.get(), acs);
        
        MutationBatch mutation = ks.prepareMutationBatch();
        
        
        
        mutation.withRow(cf, "foo").putColumn(c2, "Hello World");
        
        mutation.execute();
        
        
    }
    
    
    private static TridentTuple newTridentTuple(Fields fields, List<Object> values){
        TridentTupleView.FreshOutputFactory fof = new TridentTupleView.FreshOutputFactory(fields);
        return fof.create(values);
    }
    
    private void dumpMap(Map<SimpleComposite, String> values){
    	for (Entry<SimpleComposite, String> entry: values.entrySet()){
    		LOG.debug("Entry [" + entry.getKey().part1 + ":" + entry.getKey().part2 + "]");    		
    	}
    	
    }
}
