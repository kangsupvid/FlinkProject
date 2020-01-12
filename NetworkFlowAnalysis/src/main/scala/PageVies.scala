import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

object PageVies {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource = "C:\\Users\\kang\\Desktop\\最新复习\\文档\\FlinkProject\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv"
    env.readTextFile(resource)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
          UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
       })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior == "pv")
      .map(data=>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(0)
      .print("pv count")

    env.execute()
  }
}
