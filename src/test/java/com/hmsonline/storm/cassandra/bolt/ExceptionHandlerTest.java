package com.hmsonline.storm.cassandra.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import storm.trident.tuple.TridentTuple;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.CassandraClient;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;

@Ignore
@RunWith(MockitoJUnitRunner.class)
public class ExceptionHandlerTest {
    private static Logger LOG = LoggerFactory.getLogger(ExceptionHandlerTest.class);

    private static final String HOST = "192.168.22.85";
    private static final String KEYSPACE = "cirrus";
    private static final Class<String> COLUMN_CLASS = String.class;
    private static final String CLIENT_NAME = AstyanaxClient.class.getName();
   
    private CassandraClient getClient(String host, String keyspace){
        AstyanaxClient client = new AstyanaxClient(String.class, String.class);
        client.start("localhost", "stormks", Maps.newHashMap());
        return client;
    } 
    
    @Test(expected = IllegalStateException.class)
    public void testInvalidHost() {
        getClient("badhost", KEYSPACE);
    }
  

    @Test(expected = IllegalStateException.class)
    public void testInvalidKeyspace() {
        getClient(HOST, "badKeyspace");
    }

    @Test
    public void test() {
        CassandraClient client = getClient(HOST, KEYSPACE);
        
        TridentTuple tuple = mock(TridentTuple.class);
        TridentTupleMapper tupleMapper = mock(TridentTupleMapper.class);
        
        when(tupleMapper.mapToColumnFamily(tuple)).thenReturn("data");
        when(tupleMapper.mapToRowKey(tuple)).thenThrow(TupleMappingException.class);
        try {
            client.writeTuple(tuple, tupleMapper);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
