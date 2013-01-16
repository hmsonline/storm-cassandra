package com.hmsonline.storm.cassandra.client;

import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.hmsonline.storm.cassandra.bolt.mapper.Column;
import com.hmsonline.storm.cassandra.bolt.mapper.Columns;

public class AstyanaxClientTest {
	private static final Logger LOG = LoggerFactory.getLogger(AstyanaxClientTest.class);

    @Test
    @Ignore
    public void testLookup() throws Exception {
    	AstyanaxClient client = new AstyanaxClient(String.class, String.class);
    	client.start("localhost", "stormks", Maps.newHashMap());
    	
    	Columns<String, String> columns = client.lookup("followers", "bob");
    	for(Column col : columns){
    		LOG.error("NAME == [" + col.getKey() + "]");
    	}
    	
    	
    }

}
