import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

//输入广告点击样例类
case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
//按照省份统计的输出结果样例类
case class CountbyProvince(windowEnd:String,province:String,count:Long)
//输出黑名单报警信息
case class BlackListWarning(Userid:Long,adId:Long,msg:String)

object AdStatsticsByGeo {
  //定义侧输出流的标签
  private val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")
  def main(args: Array[String]): Unit = {
    //创建处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
   //创建处理时间依据
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)
    //设置数据源
    val url: URL = getClass.getResource("/AdClickLog.csv")
//    读取数据
    val AdClickEnventStream: DataStream[AdClickEvent] = env.readTextFile(url.getPath)
      .map(one => {
        val s: Array[String] = one.split(",")
        AdClickEvent(s(0).trim.toLong, s(1).trim.toLong, s(2).trim, s(3).trim, s(4).trim.toLong)
      }).name("map_adclickevent").uid("map_adclickevent_0")
        .assignAscendingTimestamps(_.timestamp*1000L)
    //自定义process funcation 过滤大量刷点及行为；
    AdClickEnventStream.keyBy(date=>(date.userId,date.adId))
        .process(new FilterBlackListUser(100))

      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new AdCountAgg(),new AdCountResult())
      .print()

    env.execute("ad statistics job")
  }
  class FilterBlackListUser(i: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent] {
//    定义状态，保存当前用户对当前广告的点击量
    lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count_state",classOf[Long]))
//保存是否发送给黑名单的状态
    lazy val isSentBlackList:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state",classOf[Boolean]))
    //保存定时器处罚的时间戳
    lazy val resetTimer:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state",classOf[Long]))
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      //取出count状态
      val curCount: Long = countState.value()

      //如果是第一次处理，定时器
      if(curCount == 0){
        val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24)+1)*(1000*60*60*24)
         resetTimer.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
      }
      if(curCount>=i){
        //判断是否发送过黑名单
        if(!isSentBlackList.value()){
          isSentBlackList.update(true)
          ctx.output(blackListOutputTag,BlackListWarning(value.userId,value.adId,"Click over "+i+" times today."))

        }
        return
      }
      //计数状态
      countState.update(curCount+1)
      out.collect(value)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //定时器触发，清空状态
      if(timestamp == resetTimer.value()){
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }

  }


}
//自定义聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class AdCountResult() extends WindowFunction[Long,CountbyProvince,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountbyProvince]): Unit ={
    out.collect(CountbyProvince(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
  }
}