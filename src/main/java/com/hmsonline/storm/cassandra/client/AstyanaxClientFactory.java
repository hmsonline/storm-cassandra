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
	 * @param configKey only one client will be created per config key
	 * @param conf the config map for the given configKey
	 * @return AstyanaxClient that maps to a configKey
	 */
	@SuppressWarnings("rawtypes")
	public static AstyanaxClient getInstance(String configKey, Map conf) {
		if (clients.containsKey(configKey)) {
			LOG.debug("Returning existing client for configKey " + configKey);
			return clients.get(configKey);
		} else {
			return createClient(configKey, conf);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private synchronized static AstyanaxClient createClient(String configKey, Map conf) {
		// in case > 1 factory client with same configKey arrive here
		if (clients.containsKey(configKey)) {
			return clients.get(configKey);
		}
		LOG.debug("Creating new AstyanaxClient for configKey " + configKey + " and starting with config " + conf);
		AstyanaxClient client = new AstyanaxClient();
		client.start(conf);
		clients.put(configKey, client);
		return client;
	}
}
