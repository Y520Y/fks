package com.flumekafka;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * description
 *
 * @author wdj on 2018/3/12
 */
public class MyFlume  extends AbstractSink implements Configurable{

    private KafkaProducer<String,String> producer;

    @Override
    public void configure(Context context) {
        Map<String,String> map = context.getParameters();
        Iterator iter = map.entrySet().iterator();
        //可以获取flume配置中的属性，配置文件中 producer.sinks.r.metadata.broker.list=ip1:9092,ip2:9092,ip3:9092
        String  metadataBrokerList = map.get("metadata.broker.list");
        Properties p = new Properties();
        p.setProperty("bootstrap.servers","192.168.10.150:9092,192.168.10.151:9092");
        p.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("request.required.acks","1");
        producer = new KafkaProducer<>(p);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if(event == null){
                transaction.rollback();
                status = Status.BACKOFF;
                return status;
            }
            byte[] bytes = event.getBody();
            ProducerRecord<String, String> record = new ProducerRecord("flume1",new String(bytes));
            producer.send(record);
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            transaction.rollback();
            status = Status.BACKOFF;
            e.printStackTrace();
        } finally {
            transaction.close();
        }
        return status;
    }


}
