package com.hmsonline.storm.cassandra.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.client.AstyanaxClient;

import backtype.storm.task.IMetricsContext;
import backtype.storm.utils.Utils;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class CassandraStateFactory implements StateFactory {

	private static final long serialVersionUID = 1055824326488179872L;

	private static final Logger LOG = LoggerFactory.getLogger(CassandraStateFactory.class);
	
	private TridentTupleMapper<?, ?, ?> mapper;
	private String configKey;
	
	public CassandraStateFactory(String configKey, TridentTupleMapper<?,?,?> mapper){
		this.mapper = mapper;
		this.configKey = configKey;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		LOG.debug("makeState partitionIndex:{} numPartitions:{}", partitionIndex, numPartitions);
		AstyanaxClient client = new AstyanaxClient();
		client.start((Map)conf.get(this.configKey));
		int batchMaxSize = Utils.getInt(Utils.get(conf, StormCassandraConstants.CASSANDRA_BATCH_MAX_SIZE, CassandraState.DEFAULT_MAX_BATCH_SIZE));
		return new CassandraState(client, this.mapper, batchMaxSize);
	}

}
