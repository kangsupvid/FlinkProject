import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import org.apache.flink.api.scala._

object UvwithBloom1 {
  def main(args: Array[String]): Unit = {
    //创建运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间处理类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //处理数据
    val dataStream: DataStream[UserBehavior] = env.readTextFile("C:\\Users\\kang\\Desktop\\最新复习\\文档\\FlinkProject\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(one => {
        val data: Array[String] = one.split(",")
        UserBehavior(data(0).trim.toLong, data(1).trim.toLong, data(2).trim.toInt, data(3).trim, data(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)

    val value: DataStream[UvCount] = dataStream.filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger1())
      .process(new UvcountWithBloon1())
    value.print()

    env.execute()
  }
}
//自定义窗口触发器
class MyTrigger1() extends Trigger[(String,Long),TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult= {
    //每来一条数据就直接处罚窗口操作，并清空所有状态
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ???
}
//定义一个布隆过滤器
 class Bloom1(size:Long) extends Serializable{
  //位图总大小，16M
  private val cop = if(size > 0) size else 1 << 27
  //定义哈希函数
  def hash(value:String,seed:Int):Long={
  var result : Long = 0L;
  for(i <- 0 until value.length){
    result = result * seed + value.charAt(i)
  }
    result & (cop -1)
  }
}

class UvcountWithBloon1() extends  ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
 //定义redis过滤器
  lazy val jedis = new Jedis("node01",6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit ={
    //位图的存储方式，key是windowEnd，value是bitMap
    val storeKey: String = context.window.getEnd.toString
    var count = 0L
    //把每个窗口的uv count值存入redis，有效内容window-》count 所以要先从redis读取；
    if(jedis.hget("count",storeKey) != null){
      count = jedis.hget("count",storeKey).toLong
    }
    //用布隆过滤器判断当前用户是否存在
    val userId: String = elements.last._2.toString
    val offset: Long = bloom.hash(userId,61)
    val isExist = jedis.getbit(storeKey,offset)
    if(!isExist){
      //如果不存在，位图位置+1
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,(count+1).toString)
      out.collect(UvCount(storeKey.toLong,count+1))
    }else{
      out.collect(UvCount(storeKey.toLong,count))
    }
  }




}