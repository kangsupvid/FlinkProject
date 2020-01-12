package flink


import java.util.{Date, Properties}

import bean._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import source.KafkaSource
import ut.DateUtils



object Hour  {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties: Properties = KafkaSource.getProperties("node01:9092", "groupid", "latest")
    val dateStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("abc", new SimpleStringSchema(), properties))
//        //利用fastJson工具，将接收到的数据转换成kafkaInfo类型的数据
//      val mapKafkaInfo: DataStream[KafkaInfo] = dateStream.map(data => {
//        JSON.parseObject(data, classOf[KafkaInfo])
//      })
//      //过滤到掉其他数据库，同时只保留操作类型为insert的数据。
//      val mysqlInfoDate: DataStream[MysqlInfo] = mapKafkaInfo
//        .filter(_.database== "test")
//        .filter(_.`type` == "INSERT")
//        .map(one => {
//          //将data的数据中[{xxx}]的[]去掉，得到json字符串
//          val str: String = one.`data`.drop(1).dropRight(1)
//          //将得到的json数据解析成MysqlInfo的数据流
//          JSON.parseObject(str, classOf[MysqlInfo])
//        })
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val mysqlInfoDate: DataStream[MysqlInfo] = dateStream.map(one => {
      val s: Array[String] = one.split(" ")
      MysqlInfo(s(0).trim, s(1).trim, s(2).trim, s(3).trim.toDouble, s(4))
    }).assignAscendingTimestamps(_.timestamp.toLong)


      mysqlInfoDate.map(one=> {

        val dateStream: String = DateUtils.getDateToString(one.timestamp.trim.toLong)
        val date: String = dateStream.substring(0,13)
        (date,1)
      })
        .keyBy(_._1)
        .timeWindow(Time.hours(1))
        .reduce((d1, d2) => (d1._1, d1._2.+(d2._2))).print()


    env.execute()
  }
}
