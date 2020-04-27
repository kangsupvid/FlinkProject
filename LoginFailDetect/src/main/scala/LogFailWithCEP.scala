import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object LogFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val url: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEnvent] = env.readTextFile(url.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEnvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEnvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEnvent): Long = element.eventTime * 1000L
    })
    val userIdKyeStream: KeyedStream[LoginEnvent, Long] = loginEventStream.keyBy(_.userId)
    //定义匹配模式
    val loginFailPattern: Pattern[LoginEnvent, LoginEnvent] = Pattern.begin[LoginEnvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))


    //在事件上运行cet
    val patternStream: PatternStream[LoginEnvent] = CEP.pattern(userIdKyeStream,loginFailPattern)
    //从patternStream 应用select function 检出匹配事件序列
    patternStream.select(new LoginFailMatch()).print()

    env.execute("login fail with cep job")

  }

}

class LoginFailMatch() extends PatternSelectFunction[LoginEnvent,Warining] {
  override def select(map: util.Map[String, util.List[LoginEnvent]]): Warining = {
    //从map中按照名称取出对应的事件
    val firstFail: LoginEnvent = map.get("begin").iterator().next()
    val lastFail:LoginEnvent = map.get("next").iterator().next()
    Warining(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail")
  }
}