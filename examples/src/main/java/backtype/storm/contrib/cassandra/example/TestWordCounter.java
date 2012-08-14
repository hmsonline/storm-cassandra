package backtype.storm.contrib.cassandra.example;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import java.util.HashMap;
import static backtype.storm.utils.Utils.tuple;


@SuppressWarnings("serial")
public class TestWordCounter implements IBasicBolt {

    Map<String, Integer> _counts;
    
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        _counts = new HashMap<String, Integer>();
    }
    
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValues().get(0);
        int count = 0;
        if(_counts.containsKey(word)) {
            count = _counts.get(word);
        }
        count++;
        _counts.put(word, count);
        collector.emit(tuple(word, count));
    }
    
    public void cleanup() {
        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}