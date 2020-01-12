 import java.sql.Timestamp
 import java.util.UUID
 import java.util.concurrent.TimeUnit

 import org.apache.flink.api.scala._
 import org.apache.flink.streaming.api.TimeCharacteristic
 import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
 import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
 import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
 import org.apache.flink.streaming.api.windowing.time.Time
 import org.apache.flink.streaming.api.windowing.windows.TimeWindow
 import org.apache.flink.util.Collector

 import scala.util.Random

 //输入数据样例类
case class MarketingUserBehavior1(userId:String,behavior:String,channel:String,timestamp:Long)
//输出结果样例类
 case class MarketingViewCount1(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)


object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource())
    dataStream.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data=>((data.channel,data.behavior),1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountByChannel())
      .print()
    env.execute()
  }
}

 //自定义数据源
 class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {


   //定义是否运行的标识位
   var running = true
   //定义用户行为的集合
   var behaviorTypes:Seq[String] = Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")
   //定义渠道的集合
   val channelSets:Seq[String] = Seq("wechat","weibo","appstore","huaweistore")
   //定义一个随机数发生器
   val round:Random = new Random()

   override def cancel() = running=false


   override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
     //定义一个生成数据的上限
     val maxElements = Long.MaxValue
     //生成数据个数
     var count = 0L
     //随机生成所有数据
     while(running && count<maxElements){
       val id = UUID.randomUUID().toString
       val behavior = behaviorTypes(round.nextInt(behaviorTypes.size))
       val channel = channelSets(round.nextInt(channelSets.size))
       val ts = System.currentTimeMillis()
        ctx.collect(MarketingUserBehavior(id,behavior,channel,ts))
//        count += 1;
       TimeUnit.MILLISECONDS.sleep(10L)
     }
   }
  }

 //实现自定义的处理函数
  class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow] {

   override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit ={
     val startTs = new Timestamp( context.window.getStart).toString
     val endTs = new Timestamp(context.window.getEnd).toString
     val channel = key._1
     val behavior = key._2
     val count = elements.size
     out.collect(MarketingViewCount(startTs,endTs,channel,behavior,count))
   }
 }