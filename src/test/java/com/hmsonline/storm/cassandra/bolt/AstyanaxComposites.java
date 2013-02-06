package com.hmsonline.storm.cassandra.bolt;

import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.createColumnFamily;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newClusterContext;
import static com.hmsonline.storm.cassandra.bolt.AstyanaxUtil.newContext;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.AbstractComposite.ComponentEquality;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class AstyanaxComposites {
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxComposites.class);
    private static String KEYSPACE = AstyanaxComposites.class.getSimpleName();

    @BeforeClass
    public static void setupCassandra() throws TTransportException, IOException, InterruptedException,
            ConfigurationException, Exception {
        SingletonEmbeddedCassandra.getInstance();
        try {

            AstyanaxContext<Cluster> clusterContext = newClusterContext("localhost:9160");

            createColumnFamily(clusterContext, KEYSPACE, "composite", "CompositeType(UTF8Type, UTF8Type)", "UTF8Type", "UTF8Type");
            createColumnFamily(clusterContext, KEYSPACE, "composite2", "CompositeType(UTF8Type, UTF8Type, UTF8Type)", "UTF8Type", "UTF8Type");

        } catch (Exception e) {
            LOG.warn("Couldn't setup cassandra.", e);
            throw e;
        }
    }

//    @Test
    public void simpleReadWriteCompositeTest() {
        boolean fail = false;
        try {
            AstyanaxContext<Keyspace> context = newContext("localhost:9160", KEYSPACE);
            Keyspace ks = context.getEntity();
            ColumnFamily<String, Composite> cf = new ColumnFamily<String, Composite>("composite",
                    StringSerializer.get(), CompositeSerializer.get());

            MutationBatch mutation = ks.prepareMutationBatch();

            mutation.withRow(cf, "mykey").putColumn(makeStringComposite("foo", "bar"), "Hello Composite Column Range Query");
            mutation.withRow(cf, "mykey").putColumn(makeStringComposite("foo", "baz"), "My dog has fleas");
            mutation.withRow(cf, "mykey").putColumn(makeStringComposite("fzz", "baz"), "It is snowing");

            mutation.execute();

            // simple column fetch
            ColumnFamilyQuery<String, Composite> query = ks.prepareQuery(cf);
            Column<Composite> result = query.getKey("mykey").getColumn(makeStringComposite("foo", "bar")).execute().getResult();
            LOG.debug(result.getStringValue());

            // build up a composite range query
            Composite start = makeStringEqualityComposite(new String[]{"foo"}, new ComponentEquality[]{ComponentEquality.EQUAL});

//            Composite end = new Composite();
//            end.addComponent("fyy", StringSerializer.get(), ComponentEquality.GREATER_THAN_EQUAL);
            Composite end = makeStringEqualityComposite(new String[]{"fyy"}, new ComponentEquality[]{ComponentEquality.GREATER_THAN_EQUAL});

            ColumnList<Composite> results = query.getKey("mykey")
                    .withColumnRange(start.serialize(), end.serialize(), false, 100).execute().getResult();
            LOG.debug("Query matched {} results.", results.size());
            for (Composite columnKey : results.getColumnNames()) {
                LOG.debug("Component(0): {}", columnKey.getComponent(0).getValue(StringSerializer.get()));
                LOG.debug("Component(1): {}", columnKey.getComponent(1).getValue(StringSerializer.get()));
                LOG.debug("Value: {}", results.getValue(columnKey, StringSerializer.get(), ""));
                if (results.getValue(columnKey, StringSerializer.get(), "").equals("It is snowing")) {
                    fail = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        assertFalse("unexpected result", fail);
    }
    
    
    
    @Test
    public void twoDimensionalCompositeRangeTest() {
        boolean fail = false;
        try {
            final String rowkey = "mykey2";
            AstyanaxContext<Keyspace> context = newContext("localhost:9160", KEYSPACE);
            Keyspace ks = context.getEntity();
            ColumnFamily<String, Composite> cf = new ColumnFamily<String, Composite>("composite2",
                    StringSerializer.get(), CompositeSerializer.get());

            MutationBatch mutation = ks.prepareMutationBatch();

            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("a", "a", "a"), "aaa");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("b", "b", "b"), "bbb");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("c", "c", "c"), "ccc");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("d", "d", "d"), "ddd");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("a", "a", "b"), "aab");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("a", "a", "c"), "aac");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("a", "a", "d"), "aad");
            mutation.withRow(cf, rowkey).putColumn(makeStringComposite("a", "b", "c"), "abc");
            mutation.execute();
            
            // build up a composite range query
            Composite start = makeStringEqualityComposite(new String[]{"a", "a"}, new ComponentEquality[]{ComponentEquality.EQUAL, ComponentEquality.EQUAL});

            Composite end = makeStringEqualityComposite(new String[]{"b", "b"}, new ComponentEquality[]{ComponentEquality.GREATER_THAN_EQUAL, ComponentEquality.GREATER_THAN_EQUAL});

            ColumnFamilyQuery<String, Composite> query = ks.prepareQuery(cf);
            ColumnList<Composite> results = query.getKey(rowkey)
                    .withColumnRange(start.serialize(), end.serialize(), false, 100).execute().getResult();
            LOG.debug("Query matched {} results.", results.size());
            for (Composite columnKey : results.getColumnNames()) {
                LOG.debug("Component(0): {}", columnKey.getComponent(0).getValue(StringSerializer.get()));
                LOG.debug("Component(1): {}", columnKey.getComponent(1).getValue(StringSerializer.get()));
                LOG.debug("Component(2): {}", columnKey.getComponent(2).getValue(StringSerializer.get()));
                LOG.debug("Value: {}", results.getValue(columnKey, StringSerializer.get(), ""));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        assertFalse("unexpected result", fail);
    }

    public static Composite makeStringComposite(String... values) {
        Composite comp = new Composite();
        for (String value : values) {
            comp.addComponent(value, StringSerializer.get());
        }
        return comp;
    }
    
    
    public static Composite makeStringEqualityComposite(String[] values, ComponentEquality[] equalities) {
        if(values.length != equalities.length){
            throw new IllegalArgumentException("Number of values and equalities must match.");
        }
        Composite comp = new Composite();
        for (int i = 0;i<values.length;i++) {
            comp.addComponent(values[i], StringSerializer.get(), equalities[i]);
        }
        return comp;
    }
}
