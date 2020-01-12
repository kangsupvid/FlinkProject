import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("kafka-uv")
  }
  def writeToKafka(topic: String): Unit ={
    val props = new Properties
    props.put("bootstrap.servers", "node01:9092") //kafka集群，broker-list
    props.put("acks", "all")
    props.put("retries", "1") //重试次数
    props.put("batch.size", "16384") //批次大小
    props.put("linger.ms", "1") //等待时间
    props.put("buffer.memory", "33554432") //RecordAccumulator缓冲区大小
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //定义一个kafka producer
    val producer = new KafkaProducer[String,String](props)
    //从文件中读取数据，发送
    val bufferedSource: BufferedSource = io.Source.fromFile("C:\\Users\\kang\\Desktop\\最新复习\\文档\\FlinkProject\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      val value: ProducerRecord[String, String] = new ProducerRecord[String,String](topic,line)
      producer.send(value)
    }
    producer.close()


  }
}
