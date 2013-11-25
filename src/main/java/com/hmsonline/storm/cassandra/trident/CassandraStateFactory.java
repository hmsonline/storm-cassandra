package com.hmsonline.storm.cassandra.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;
import backtype.storm.utils.Utils;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;
import com.hmsonline.storm.cassandra.client.AstyanaxClientFactory;
import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;

public class CassandraStateFactory implements StateFactory {

    private static final long serialVersionUID = 1055824326488179872L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraStateFactory.class);

    private String cassandraClusterId;
    private ExceptionHandler exceptionHandler;

    /**
     * @param  Identifier that uniquely identifies the Cassandra Cluster
     */
    public CassandraStateFactory(String cassandraClusterId) {
        this(cassandraClusterId, null);
    }

    /**
     * @param cassandraClusterId Identifier that uniquely identifies the Cassandra Cluster
     * @param exceptionHandler
     */
    public CassandraStateFactory(String cassandraClusterId, ExceptionHandler exceptionHandler) {
        this.cassandraClusterId = cassandraClusterId;
        this.exceptionHandler = exceptionHandler;
    }

    /* (non-Javadoc)
     * @see storm.trident.state.StateFactory#makeState(java.util.Map, backtype.storm.task.IMetricsContext, int, int)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("Making new CassandraState object for cluster " + cassandraClusterId + ": partition [" + partitionIndex + "] of [" + numPartitions + "]");
        AstyanaxClient client = AstyanaxClientFactory.getInstance(cassandraClusterId, (Map)conf.get(cassandraClusterId));
        int batchMaxSize = Utils.getInt(Utils.get(conf, StormCassandraConstants.CASSANDRA_BATCH_MAX_SIZE,
                CassandraState.DEFAULT_MAX_BATCH_SIZE));
        return new CassandraState(client, batchMaxSize, this.exceptionHandler);
    }

}
