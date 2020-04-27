package com.likang.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.logging.SimpleFormatter;

/**
 * @program: FlinkProject
 * @description:
 * @author: likang
 * @create: 2020-02-05 18:31
 **/
public class PKKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "example";

        //通过死循环一直不停往kafka的broker里面生产数据
        while(true) {
            final StringBuilder builder = new StringBuilder();
            builder.append("imooc").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")//域名
                    .append(getTraffic()).append("\t");//流量

            System.out.println(builder.toString());
            producer.send(new ProducerRecord<String,String>(topic,builder.toString()));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        }

    public static String getLevels(){
        String[] levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }
    public static String getIps(){
        String[] ips = new String[]{
                "223,103.11.213",
                "213,103.21.113",
                "123,113.31.213",
                "123,103.41.214",
                "223,113.51.215",
                "123,133.61.216",
                "223,143.71.217"};
        return ips[new Random().nextInt(ips.length)];
        }
    public static String getDomains(){
        String[] domains = new String[]{
                "www.baidu.com",
                "www.wagyi.com",
                "www.youku.com",
                "www.aiqiy.com",
                "www.likan.com",
                 };
        return domains[new Random().nextInt(domains.length)];
    }
    public static Long getTraffic(){
        return Long.valueOf(new Random().nextInt(10000));
    }

}
