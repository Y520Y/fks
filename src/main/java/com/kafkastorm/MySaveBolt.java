package com.kafkastorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * description
 *
 * @author wdj on 2018/3/13
 */
public class MySaveBolt extends BaseRichBolt {

    OutputCollector collector;

    Map<String,Long> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
          this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> objects = tuple.getValues();
        Long curNum =map.get(objects.get(0));
        curNum = curNum == null?0L:curNum;
        map.put(objects.get(0).toString(),curNum+1);
        System.out.println(objects.get(0)+"当前个数=="+(curNum+1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
