package com.kafkastorm;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * description
 *
 * @author wdj on 2018/3/13
 */
public class MySpout extends KafkaSpout {

    SpoutOutputCollector collector;
    Map<String,Long> map = new HashMap<>();

    public MySpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
           this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String id = UUID.randomUUID().toString();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
