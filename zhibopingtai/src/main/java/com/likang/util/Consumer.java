package com.likang.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @program: FlinkProject
 * @description:
 * @author: likang
 * @create: 2020-02-05 19:03
 **/
public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");
        props.put("group.id", "kafka-kudu-test"); //消费者组，只要group.id相同，就属于同一个消费者组

        props.put("enable.auto.commit", "false");//自动提交offset
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final ArrayList<String> list = new ArrayList<String>();
        list.add("example");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(list);

        while (true){
            final ConsumerRecords<String, String> recores = consumer.poll(1000);
            for(ConsumerRecord<String,String> s : recores){
                System.out.println(s.key()+":"+s.value());
            }
        }
    }
}
