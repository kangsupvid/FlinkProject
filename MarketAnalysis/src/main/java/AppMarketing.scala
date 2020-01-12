

 import java.sql.Timestamp

 import org.apache.flink.api.common.functions.AggregateFunction
 import org.apache.flink.api.scala._
 import org.apache.flink.streaming.api.TimeCharacteristic
 import org.apache.flink.streaming.api.scala.function.WindowFunction
 import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
 import org.apache.flink.streaming.api.windowing.time.Time
 import org.apache.flink.streaming.api.windowing.windows.TimeWindow
 import org.apache.flink.util.Collector


 //输入数据样例类
case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)
//输出结果样例类
 case class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)


object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource())
    dataStream.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data=>("dummyKey",1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .aggregate(new CountAgg(),new MarketingCountTotal())
      .print()
    env.execute()
  }
}

 class CountAgg() extends AggregateFunction[(String,Long),Long,Long] {
   override def createAccumulator(): Long = 0L

   override def add(in: (String, Long), acc: Long): Long = acc + 1

   override def getResult(acc: Long): Long = acc

   override def merge(acc: Long, acc1: Long): Long = acc + acc1
 }

 class MarketingCountTotal() extends WindowFunction[Long,MarketingViewCount,String,TimeWindow] {
   override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit ={
     val startTs = new Timestamp(window.getStart).toString
     var endTs = new Timestamp(window.getEnd).toString
     val count = input.iterator.next()
     out.collect(MarketingViewCount(startTs,endTs,"app marketing","count" ,count))
   }

 }