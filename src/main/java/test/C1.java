package test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class C1 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("group.id", "kafka-kudu-test");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("enable.auto.commit", "false");//自动提交offset

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("abc"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);


                    for (ConsumerRecord<String, String> record : records){
                        if(records != null) {
                            String[] split = record.value().split("-");
                            System.out.println(split[0]);
                            System.out.println(split[1]);

                            new K1().run(Integer.parseInt(split[0]), Integer.parseInt(split[0]));
                            }
                consumer.commitSync();



            }
        }
    }

}