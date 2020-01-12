package com.mouse.redisPool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class P1 {

    public static void main(String[] args){
        Random random = new Random();
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//hour: String, user_id: String, site_id: String
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            String hour = String.valueOf(random.nextInt(12));
            String user_id = String.valueOf(random.nextInt(100));
            String site_id = String.valueOf(random.nextInt(100));
            String s = hour+","+user_id+","+site_id;

            producer.send(new ProducerRecord<String, String>("redis",0 ,i+"",s));
        }
        producer.close();
    }
}
