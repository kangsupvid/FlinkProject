package newp

import java.util
import java.util.{Arrays, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

object CustomConsumer1 {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties
    props.put("bootstrap.servers", "node01:9092")
    props.put("group.id", "kafka-kudu-test") //消费者组，只要group.id相同，就属于同一个消费者组

    props.put("enable.auto.commit", "false") //自动提交offset


    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("kafka-kudu"))
    while ( {
      true
    }) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      import scala.collection.JavaConversions._
      for (record <- records) {

        var split: Array[String] = record.value.split("-")
        println(split(0))
        println(split(1))
        var k: KafkaKudu = new KafkaKudu
        if (k != null)
          k.run(split(0).toLong, split(1).toDouble)
      }
      consumer.commitSync()
    }
  }


}
