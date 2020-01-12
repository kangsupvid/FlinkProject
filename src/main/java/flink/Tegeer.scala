package flink

 import java.lang

 import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
 import org.apache.flink.streaming.api.TimeCharacteristic
 import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
 import org.apache.flink.streaming.api.windowing.time.Time
 import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
 import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
 import org.apache.flink.api.scala._
 import ut.DateUtils
case class ap(id:String,timestamp:Long)
object Tegeer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    env.socketTextStream("node01",7777)
      .map(one=>{
        val s: Array[String] = one.split(" ")
        ap(s(0),s(1).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .map(data => (data.id,1))
      .keyBy(0)
      .timeWindow(Time.hours(1))
//     .trigger(new MyTrigger1())
//      .reduce((a,b)=>(a._1,a._2+b._2))

  }
}

class MyTrigger1() extends Trigger[(String,Long),Window] {
  private val stateDescriptor = new ValueStateDescriptor[Integer]("total",classOf[Integer])
  override def onElement(element: (String, Long), timestamp: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {

    val sumState: ValueState[Integer] = ctx.getPartitionedState(stateDescriptor)
    if (null == sumState.value()) sumState.update(0)
    sumState.update(1 + sumState.value())
    if (sumState.value() >= 4) {
      println("触发数据条数为：" + (1 + sumState.value()))
      return TriggerResult.FIRE
    }
    val todayZeroPointTimeStamps: lang.Long = DateUtils.getTodayZeroPointTimestamps(window.maxTimestamp())
    println("todayZeroPointTimeStamps:"+todayZeroPointTimeStamps)
    println("timeStamp:"+timestamp)
    println("window.maxTimestamp"+window.maxTimestamp())
    if(timestamp>=todayZeroPointTimeStamps){
      return TriggerResult.FIRE_AND_PURGE
    }
    if(timestamp >= window.maxTimestamp()){
      return TriggerResult.FIRE_AND_PURGE
    }else{
      return TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult =  TriggerResult.CONTINUE

  override def clear(window: Window, ctx: Trigger.TriggerContext){
    println("清理窗口状态  窗口内保存值为" + ctx.getPartitionedState(stateDescriptor).value())
    ctx.getPartitionedState(stateDescriptor).clear()
  }
}






