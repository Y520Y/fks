package com.kafkastorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * description
 *
 * @author wdj on 2018/3/13
 */
public class MyCountBolt extends BaseRichBolt {

    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String message = tuple.getString(0);
        if(message == null || message.lastIndexOf(":")==-1){
            return;
        }
        String m = message.substring(0,message.lastIndexOf(":"));
        collector.emit(new Values(m,1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("m","num"));
    }
}
