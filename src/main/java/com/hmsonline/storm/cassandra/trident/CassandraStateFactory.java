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

    private String configKey;
    private ExceptionHandler exceptionHandler;

    public CassandraStateFactory(String configKey){
        this(configKey, null);
    }

    public CassandraStateFactory(String configKey, ExceptionHandler exceptionHandler) {
        this.configKey = configKey;
        this.exceptionHandler = exceptionHandler;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.debug("makeState partitionIndex:{} numPartitions:{}", partitionIndex, numPartitions);
        AstyanaxClient client = AstyanaxClientFactory.getInstance(configKey, (Map)conf.get(configKey));
        int batchMaxSize = Utils.getInt(Utils.get(conf, StormCassandraConstants.CASSANDRA_BATCH_MAX_SIZE,
                CassandraState.DEFAULT_MAX_BATCH_SIZE));
        return new CassandraState(client, batchMaxSize, this.exceptionHandler);
    }

}
