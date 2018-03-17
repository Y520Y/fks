package com.kafkastorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * description
 *
 * @author wdj on 2018/3/13
 */
public class MyKafka {
    
    public static void main(String[] args){
         String topic="flume1";
//        进度信息记录于zookeeper的哪个路径下
         String zkRoot="/kafka-storm";
//        进度记录的id，想要一个新的Spout读取之前的记录，应把它的id设为跟之前的一样。同一个id，同一个spout
         String id="old";
        //配置kafka时，如果使用zookeeper create /kafka创建了节点，kafka与storm集成时new ZkHosts(zks) 需要改成 new ZkHosts(zks,”/kafka/brokers”),不然会报
        //java.lang.RuntimeException: java.lang.RuntimeException: org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /brokers/topics/test/partitions
//        BrokerHosts brokerHosts = new ZkHosts("192.168.10.150:2181,192.168.10.151:2181","/kafka/brokers");

        //单经过测试，上个方法还是报错那个错误。
        //ZkHosts 用于动态跟踪Kafka Borker的partition映射
        //KafkaSpout会执行建立zookeeper客户端，在zookeeper zk_root + "/topics/" + _topic + "/partitions" 路径下获取到partition列表
        BrokerHosts brokerHosts = new ZkHosts("192.168.10.150:2181,192.168.10.151:2181");

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,zkRoot,id);
        spoutConfig.socketTimeoutMs = 60 * 1000;  //与Kafka broker的连接的socket超时时间
        //ignoreZkOffsets 为false时，再配置zkPort以及zkServers，现在启动时不会重新读取数据。俩个配置缺一则重新开始读取
        spoutConfig.ignoreZkOffsets=false;
//        spoutConfig.forceFromStart =false;
        spoutConfig.zkPort = 2181;
        List<String> servers = new ArrayList<>();
        servers.add("192.168.10.150");
        servers.add("192.168.10.151");
        spoutConfig.zkServers = servers;

        //从Kafka中取出的byte[]，该如何反序列化
        // 定义输出为String
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        //可以使用KafkaSpout，也可以使用自定义Spount继承KafkaSpout重写ack方法
        //KafkaSpout默认ack失败时，判断是否在质保内以及保质期个数未超标，则重发，详见有道云笔记
        builder.setSpout("mySpout",new KafkaSpout(spoutConfig),1);
        builder.setBolt("wordCount",new MyCountBolt(),3).shuffleGrouping("mySpout");
        builder.setBolt("saveBolt",new MySaveBolt(),3).fieldsGrouping("wordCount",new Fields("m"));

        Config config = new Config();
        config.setNumWorkers(4);
        config.setNumAckers(0);
        config.setDebug(false);

        if(args == null || args.length==0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("myTopoly",config,builder.createTopology());
        }else{
            try {
                StormSubmitter.submitTopology("myTopoly",config,builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    
}
