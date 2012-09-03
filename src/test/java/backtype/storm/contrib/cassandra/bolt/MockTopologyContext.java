package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.HashMap;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class MockTopologyContext extends TopologyContext {
    
    private Fields declaredFields;

    public MockTopologyContext(StormTopology topology, Fields fields) {
        super(topology, new HashMap<String, String>(), new HashMap<Integer, String>(), null, null, null, null, null,
                -1, -1, new ArrayList<Integer>(), null, null, null);
        this.declaredFields = fields;
    }

    public Fields getComponentOutputFields(String componentId, String streamId) {
        return this.declaredFields;
    }
}
