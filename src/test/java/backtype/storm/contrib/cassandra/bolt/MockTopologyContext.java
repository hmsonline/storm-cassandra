package backtype.storm.contrib.cassandra.bolt;

import java.util.HashMap;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class MockTopologyContext extends TopologyContext {

	public MockTopologyContext(StormTopology topology) {
		super(topology, new HashMap<Integer, String>(), null, null, null, null);		
	}
	
    public Fields getComponentOutputFields(String componentId, String streamId) {
        return new Fields("VALUE");
    }
}
