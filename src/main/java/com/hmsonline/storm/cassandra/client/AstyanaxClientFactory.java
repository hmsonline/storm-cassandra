package com.hmsonline.storm.cassandra.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jtyack
 * Factory ensures that one AstyanaxClient exists per configKey
 *
 */
public class AstyanaxClientFactory {
	
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxClientFactory.class);
	
	@SuppressWarnings("rawtypes")
	private static Map<String, AstyanaxClient> clients = new ConcurrentHashMap<String, AstyanaxClient>();

	// factory clients cannot instantiate
	private AstyanaxClientFactory(){
    }
	
	/**
	 * @param cassandraClusterId only one instance will be available per Cassandra Cluster identifer
	 * @param conf the configuration for the given cassandraClusterId
	 * @return AstyanaxClient that maps to a cassandraClusterId
	 */
	@SuppressWarnings("rawtypes")
	public static AstyanaxClient getInstance(String cassandraClusterId, Map conf) {
		if (clients.containsKey(cassandraClusterId)) {
			LOG.debug("Returning existing instance that maps to cassandra cluster " + cassandraClusterId);
			return clients.get(cassandraClusterId);
		} else {
			return createClient(cassandraClusterId, conf);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private synchronized static AstyanaxClient createClient(String cassandraClusterId, Map conf) {
		// in case > 1 factory client with same cassandraClusterId arrive here
		if (clients.containsKey(cassandraClusterId)) {
			LOG.debug("Returning existing instance that maps to cassandra cluster " + cassandraClusterId);
			return clients.get(cassandraClusterId);
		}
		LOG.debug("Creating new AstyanaxClient instance for cassandra cluster " + cassandraClusterId + " and starting with config " + conf);
		AstyanaxClient client = new AstyanaxClient();
		client.start(conf);
		clients.put(cassandraClusterId, client);
		return client;
	}
}
