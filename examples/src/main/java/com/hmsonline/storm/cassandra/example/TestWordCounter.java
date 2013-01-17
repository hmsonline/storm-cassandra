package com.hmsonline.storm.cassandra.example;

import static backtype.storm.utils.Utils.tuple;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class TestWordCounter implements IBasicBolt {

    Map<String, Integer> counts;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        this.counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValues().get(0);
        int count = 0;
        if (this.counts.containsKey(word)) {
            count = this.counts.get(word);
        }
        count++;
        this.counts.put(word, count);
        collector.emit(tuple(word, count));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}