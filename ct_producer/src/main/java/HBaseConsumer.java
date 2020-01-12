import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class HBaseConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");
        props.put("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("enable.auto.commit", "false");//自动提交offset

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        HBaseDAO hBaseDAO = new HBaseDAO();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                hBaseDAO.put(record.value());
            }
            consumer.commitSync();
        }




//        // 创建配置对象
//        ConsumerConfig consumerConfig = new ConsumerConfig(PropertiesUtil.properties);
//        // 得到当前消费主题
//        String callLogTopic = PropertiesUtil.getProperty("topic");
//
//        // 订阅主题，开始消费
//        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
//        Map<String, Integer> topicMap = new HashMap<String, Integer>();
//        topicMap.put(callLogTopic, 1);
//
//        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
//        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
//
//        Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector.createMessageStreams(topicMap, keyDecoder, valueDecoder);
//        KafkaStream<String, String> stream = consumerMap.get(callLogTopic).get(0);
//        ConsumerIterator<String, String> it = stream.iterator();
//        HBaseDAO hBaseDAO = new HBaseDAO();
//        while (it.hasNext()) {
//            // 将消息实时写入到Hbase中
//            String msg = it.next().message();
//            System.out.println(msg);
//            hBaseDAO.put(msg);
//        }
    }
}


