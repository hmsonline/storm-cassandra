package com.hmsonline.storm.cassandra.trident;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraMapState<T> implements IBackingMap<T> {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CassandraMapState.class);

    @SuppressWarnings("rawtypes")
    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    public static final String CASSANDRA_CLUSTER_NAME = "cassandra.clusterName";
    public static final String ASTYANAX_CONFIGURATION = "astyanax.configuration";
    public static final String ASTYANAX_CONNECTION_POOL_CONFIGURATION = "astyanax.connectionPoolConfiguration";
    public static final String ASTYANAX_CONNECTION_POOL_MONITOR = "astyanax.connectioPoolMonitor";

    private final Map<String, Object> DEFAULTS = new ImmutableMap.Builder<String, Object>()
            .put(CASSANDRA_CLUSTER_NAME, "ClusterName")
            .put(ASTYANAX_CONFIGURATION, new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
            .put(ASTYANAX_CONNECTION_POOL_CONFIGURATION,
                    new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1))
            .put(ASTYANAX_CONNECTION_POOL_MONITOR, new CountingConnectionPoolMonitor()).build();

    private Options<T> options;
    private Serializer<T> serializer;
    protected Keyspace keyspace;

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    protected AstyanaxContext<Keyspace> createContext(Map<String, Object> config) {
        Map<String, Object> settings = Maps.newHashMap();
        for (Map.Entry<String, Object> defaultEntry : DEFAULTS.entrySet()) {
            if (config.containsKey(defaultEntry.getKey())) {
                settings.put(defaultEntry.getKey(), config.get(defaultEntry.getKey()));
            } else {
                settings.put(defaultEntry.getKey(), defaultEntry.getValue());
            }
        }
        // in the defaults case, we don't know the seed hosts until context
        // creation time
        if (settings.get(ASTYANAX_CONNECTION_POOL_CONFIGURATION) instanceof ConnectionPoolConfigurationImpl) {
            ConnectionPoolConfigurationImpl cpConfig = (ConnectionPoolConfigurationImpl) settings
                    .get(ASTYANAX_CONNECTION_POOL_CONFIGURATION);
            cpConfig.setSeeds((String) config.get(StormCassandraConstants.CASSANDRA_HOST));
        }

        return new AstyanaxContext.Builder()
                .forCluster((String) settings.get(CASSANDRA_CLUSTER_NAME))
                .forKeyspace((String) config.get(StormCassandraConstants.CASSANDRA_STATE_KEYSPACE))
                .withAstyanaxConfiguration((AstyanaxConfiguration) settings.get(ASTYANAX_CONFIGURATION))
                .withConnectionPoolConfiguration(
                        (ConnectionPoolConfiguration) settings.get(ASTYANAX_CONNECTION_POOL_CONFIGURATION))
                .withConnectionPoolMonitor((ConnectionPoolMonitor) settings.get(ASTYANAX_CONNECTION_POOL_MONITOR))
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    @SuppressWarnings("serial")
    public static class Options<T> implements Serializable {

        public Serializer<T> serializer = null;
        public int localCacheSize = 5000;
        public String globalKey = "globalkey";
        public String columnFamily = "cassandra_state";
        public String columnName = "default_cassandra_state";
        public String clientConfigKey = "cassandra.config";
        public Integer ttl = 86400; // 1 day

    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque() {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(Options<OpaqueValue> opts) {
        return new Factory(StateType.OPAQUE, opts);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional() {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional() {
        Options<Object> options = new Options<Object>();
        return nonTransactional(options);
    }

    public static StateFactory nonTransactional(Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, opts);
    }

    protected static class Factory implements StateFactory {
        private static final long serialVersionUID = -2644278289157792107L;
        private StateType stateType;
        private Options<?> options;

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public Factory(StateType stateType, Options options) {
            this.stateType = stateType;
            this.options = options;

            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (this.options.serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            CassandraMapState state = new CassandraMapState(options, conf);

            CachedMap cachedMap = new CachedMap(state, options.localCacheSize);

            MapState mapState;
            if (stateType == StateType.NON_TRANSACTIONAL) {
                mapState = NonTransactionalMap.build(cachedMap);
            } else if (stateType == StateType.OPAQUE) {
                mapState = OpaqueMap.build(cachedMap);
            } else if (stateType == StateType.TRANSACTIONAL) {
                mapState = TransactionalMap.build(cachedMap);
            } else {
                throw new RuntimeException("Unknown state type: " + stateType);
            }

            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public CassandraMapState(Options<T> options, Map conf) {
        this.options = options;
        this.serializer = options.serializer;
        AstyanaxContext<Keyspace> context = createContext((Map<String, Object>) conf.get(options.clientConfigKey));
        context.start();
        this.keyspace = context.getEntity();
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        Collection<Composite> keyNames = toKeyNames(keys);
        ColumnFamily<Composite, String> cf = new ColumnFamily<Composite, String>(this.options.columnFamily,
                CompositeSerializer.get(), StringSerializer.get());
        RowSliceQuery<Composite, String> query = this.keyspace.prepareQuery(cf).getKeySlice(keyNames);

        Rows<Composite, String> result = null;
        try {
            result = query.execute().getResult();
        } catch (ConnectionException e) {
            //TODO throw a specific error.
            throw new RuntimeException(e);
        }
        Map<List<Object>, byte[]> resultMap = new HashMap<List<Object>, byte[]>();
        if (result != null) {
            Collection<Composite> rowKeys = result.getKeys();
            for (Composite rowKey : rowKeys) {
                List<Object> dimensions = new ArrayList<Object>();
                for (int i = 0; i < rowKey.size(); i++) {
                    dimensions.add(rowKey.get(i, StringSerializer.get()));
                }
                resultMap.put(dimensions, result.getRow(rowKey).getColumns().getByteArrayValue(this.options.columnName, null));
            }
        }

        List<T> values = new ArrayList<T>();
        for (List<Object> key : keys) {
            byte[] bytes = resultMap.get(key);
            if (bytes != null) {
                values.add(serializer.deserialize(bytes));
            } else {
                values.add(null);
            }
        }

        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        MutationBatch mutation = this.keyspace.prepareMutationBatch();
        ColumnFamily<Composite, String> cf = new ColumnFamily<Composite, String>(this.options.columnFamily,
                CompositeSerializer.get(), StringSerializer.get());

        for (int i = 0; i < keys.size(); i++) {
            Composite keyName = toKeyName(keys.get(i));
            byte[] bytes = serializer.serialize(values.get(i));
            if (options.ttl != null && options.ttl > 0) {
                mutation.withRow(cf, keyName).putColumn(this.options.columnName, bytes, options.ttl);
            } else {
                mutation.withRow(cf, keyName).putColumn(this.options.columnName, bytes);
            }
        }
        try {
            mutation.execute();
        } catch (ConnectionException e) {
            throw new RuntimeException("Batch mutation for state failed.", e);
        }
    }

    private Collection<Composite> toKeyNames(List<List<Object>> keys) {
        return Collections2.transform(keys, new Function<List<Object>, Composite>() {
            @Override
            public Composite apply(List<Object> key) {
                return toKeyName(key);
            }
        });
    }

    private Composite toKeyName(List<Object> key) {
        Composite keyName = new Composite();
        for (Object component : key) {
            if (component == null) {
                component = "[NULL]";
            }
            keyName.addComponent(component.toString(), StringSerializer.get());
        }
        return keyName;
    }
}
