package com.scala.analysis

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object LogAnalysis {
  val logger = LoggerFactory.getLogger("LogAnalysis")
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val topic:String = "example"

    /**
      * 1、在生产上进行业务处理的时候，一定要考虑处理的健壮性以及你的数据的准确性
      * 脏数据或者不符合业务规则的数据是需要全部过滤之后在进行相应业务逻辑处理
      * 2、对于我们的业务来说，我们只需要统计level= E 的即可
      * 对于level非E的，不作为我们业务指标的统计范畴
      * 3、数据清洗：就是按照我们的业务规则吧原始输入的数据进行一定业务规则的处理
      * 使满足我们的业务需求为准
      */
    val dataStream = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))
    val logData= dataStream.map(x => {
      val splits: Array[String] = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0L

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $timeStr", e.getMessage)
        }
      }

      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    })
      .filter(_._2 != 0)
      .filter(_._1 == "E")
      .map(x => {
        (x._2, x._3, x._4) //(1 level(抛弃) 2 time 3 domain 4 traffic)
      })


    val value: DataStream[(Long, String, Long)] = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      val maxOutOfOrderness = 10000L
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })


    val k2Stream1: KeyedStream[(Long, String, Long), String] = value.keyBy(_._2)
    val k2Stream2: KeyedStream[(Long, String, Long), Tuple] = value.keyBy(2)
    value.print("value")
    k2Stream1.print("k2Stream1")
    k2Stream2.print("k2Stream2")
//      .keyBy(1)//此处按照域名进行key的
//        .window(TumblingEventTimeWindows.of(Time.seconds(10)))

//        .apply(new WindowFunction[(Long,String,Long),(String,String,Long),Tuple,TimeWindow] {
//          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit ={
//            val domain = key.getField(0).toString
//            var sum = 0L
//
//            val iterable = input.iterator
//            while(iterable.hasNext){
//              val next = iterable.next()
//              sum += next._3
//              println("sum="+sum)
//            }
//
//            val time =new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(window.getEnd.toString)
//            out.collect(time,domain,sum)//（时间，域名，traffic的和
//          }
//        })


    env.execute()
  }
}
