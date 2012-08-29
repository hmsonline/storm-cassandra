package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class EmbeddedCassandra {
    public static final String TEST_KS = "test_ks";
    public static final String TEST_CF = "users";
    private static Logger LOG = LoggerFactory.getLogger(EmbeddedCassandra.class);
    private static boolean started = false;

    public EmbeddedCassandra() throws Exception {
        System.setProperty("log4j.defaultInitOverride", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        if (!started) {
            CassandraDaemon cassandraService = new CassandraDaemon();
            cassandraService.activate();
            try {
                loadDataSchema(TEST_KS, Arrays.asList(TEST_CF));
            } catch (Throwable t) {
                LOG.debug("Received error when bootstrapping data schema, most likely it exists already."
                        + t.getMessage());
            }
            started = true;
        }

    }

    private void loadDataSchema(String keyspaceName, List<String> colFamilyNames) {
        List<KSMetaData> schema = new ArrayList<KSMetaData>();
        Class<? extends AbstractReplicationStrategy> strategyClass = SimpleStrategy.class;
        Map<String, String> strategyOptions = KSMetaData.optsWithRF(1);

        CFMetaData[] cfDefs = new CFMetaData[colFamilyNames.size()];
        for (int i = 0; i < colFamilyNames.size(); i++) {
            CFMetaData cfDef = new CFMetaData(keyspaceName, colFamilyNames.get(i), ColumnFamilyType.Standard,
                    UTF8Type.instance, null);
            cfDefs[i] = cfDef;
        }

        KSMetaData validKsMetadata = KSMetaData.testMetadata(keyspaceName, strategyClass, strategyOptions, cfDefs);
        schema.add(validKsMetadata);

        Schema.instance.load(schema);
        LOG.debug("======================= LOADED DATA SCHEMA FOR TESTS ==========================");
    }

    public Map<String, Map<String, String>> getRows() throws ConnectionException {
        Map<String, Map<String, String>> rows = new HashMap<String, Map<String, String>>();
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace(TEST_KS)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1)
                                .setSeeds("localhost:9160"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();

        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(TEST_CF, StringSerializer.get(),
                StringSerializer.get());

        OperationResult<Rows<String, String>> result = keyspace.prepareQuery(columnFamily).getKeySlice().execute();

        // Iterate rows and their columns
        for (Row<String, String> row : result.getResult()) {
            System.out.println(row.getKey());
            Map<String, String> cols = new HashMap<String, String>();
            for (Column<String> column : row.getColumns()) {
                System.out.println(column.getName());
                cols.put(column.getName(), column.getStringValue());
            }
            rows.put(row.getKey(), cols);
        }
        return rows;
    }

}
