package com.myStormProject.WordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCount extends BaseRichBolt {
    private OutputCollector collector;
    HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        wordCounts.put(word, wordCounts.containsKey(word) ? wordCounts.get(word) + 1 : 1);
        collector.emit(new Values(word, wordCounts.get(word)));
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("wordCounts"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
