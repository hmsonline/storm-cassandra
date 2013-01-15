package com.hmsonline.storm.cassandra.client;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientPool {
    private static final Logger LOG = LoggerFactory.getLogger(ClientPool.class);

    private static Map<String, Map<Class, CassandraClient>> clientPool = new HashMap<String, Map<Class, CassandraClient>>();
    
    public static CassandraClient getClient(String host, String keyspace, Class columnNameClass, String clientClass){
        Map<Class, CassandraClient> hostPool = clientPool.get(host);
        if (hostPool == null){
            hostPool = new HashMap<Class, CassandraClient>();
            clientPool.put(host, hostPool);
        }
        
        CassandraClient client = hostPool.get(columnNameClass);
        if (client == null || (!client.getClientKeySpace().equals(keyspace))) {
            client = createClient(host, keyspace, columnNameClass, clientClass);
            hostPool.put(columnNameClass, client);
        }
        return client;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static CassandraClient createClient(String host, String keyspace, Class columnClass, String clientClass) {
        LOG.debug("Creating Cassandra Client @ (" + host + ") for [" + columnClass.getSimpleName() + "]");

        CassandraClient cassandraClient = null;
        try {
            if (clientClass == null) {
                clientClass = "com.hmsonline.storm.cassandra.client.AstyanaxClient";
            }
            Class cl = Class.forName(clientClass);
            cassandraClient = (CassandraClient)cl.newInstance();
            cassandraClient.setColumnNameClass(columnClass);
            cassandraClient.start(host, keyspace);
        } catch (Throwable e) {
            LOG.warn("Preparation failed.", e);
            throw new IllegalStateException("Failed to prepare CassandraBolt", e);
        }
        return cassandraClient;
    }
}
