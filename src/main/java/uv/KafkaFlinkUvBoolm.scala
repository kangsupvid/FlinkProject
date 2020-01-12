package uv

import java.util.Properties

import bean._
import com.alibaba.fastjson.JSON
import flink.{Bloom, MyFlinkKudu}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import source.KafkaSource



case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)
case  class UvCount2(dayuv:String,uvCount:Long)

object KafkaFlinkUvBoolm  {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties: Properties = KafkaSource.getProperties("node01:9092", "groupid", "latest")
    val dateStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("kafka-uv", new SimpleStringSchema(), properties))

        //利用fastJson工具，将接收到的数据转换成kafkaInfo类型的数据
      val mapKafkaInfo: DataStream[KafkaInfo] = dateStream.map(data => {
        JSON.parseObject(data, classOf[KafkaInfo])
      })
      //过滤到掉其他数据库，同时只保留操作类型为insert的数据。
      val mysqlInfoDate: DataStream[MysqlInfo] = mapKafkaInfo
        .filter(_.database== "test")
        .filter(_.`type` == "INSERT")
        .map(one => {
          //将data的数据中[{xxx}]的[]去掉，得到json字符串
          val str: String = one.`data`.drop(1).dropRight(1)
          //将得到的json数据解析成MysqlInfo的数据流
          JSON.parseObject(str, classOf[MysqlInfo])
        })
//    val value: DataStream[String] = mysqlInfoDate.map(data => ("uid", data.uid))
//      .keyBy(_._1)
//      .timeWindow(Time.hours(24))
//      .trigger(new MyTrigger2())
//      .process(new UvCountWithBloom2())
//        .map(one=>one.uvCount.toString)

    val value: DataStream[UvCount2] = mysqlInfoDate.map(data => ("uid", data.uid))
      .keyBy(_._1)
      .timeWindow(Time.hours(24))
      .trigger(new MyTrigger2())
      .process(new UvCountWithBloom2())


    value.print()
    value.addSink(new MyFlinkKudu11())

//    value.addSink(new FlinkKafkaProducer011[String]("node01:9092", "kafka-uv", new SimpleStringSchema()))
     env.execute()
  }
}



// 自定义窗口触发器
class MyTrigger2() extends Trigger[(String, String), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, String), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

class UvCountWithBloom2() extends ProcessWindowFunction[(String, String), UvCount2, String, TimeWindow]{
  // 定义redis连接
  lazy val jedis = new Jedis("node01", 6379)
  lazy val bloom = new Bloom(1<<29)

  override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[UvCount2]): Unit = {
    // 位图的存储方式，key是windowEnd，value是bitmap
    val storeKey = elements.last._1
    var count = 0L
    // 把每个窗口的uv count值也存入名为count的redis表，存放内容为（windowEnd -> uvCount），所以要先从redis中读取
    if( jedis.hget("count", storeKey) != null ){
      count = jedis.hget("count", storeKey).toLong
    }
    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    // 定义一个标识位，判断reids位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
      // 如果不存在，位图对应位置1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect( UvCount2(storeKey, count + 1) )
    } else {
      out.collect( UvCount2(storeKey, count) )
    }
  }
}


// 定义一个布隆过滤器
class Bloom2(size: Long) extends Serializable {
  // 位图的总大小，默认16M
  private val cap = if (size > 0) size else 1 << 27

  // 定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }
    result  & ( cap - 1 )
  }
}

