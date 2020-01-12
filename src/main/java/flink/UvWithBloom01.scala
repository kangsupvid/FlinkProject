package flink

import java.lang
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import ut.DateUtils
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/23 11:34
  */
case class us(userId: Long, timestamp: Long )
object UvWithBloom01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.socketTextStream("node01",7777)
      .map(data => {
        val dataArray = data.split(",")

        println("传入的时间戳为："+dataArray(1).trim.toLong +"---"+DateUtils.getDateToString(dataArray(1).trim.toLong))
        us(dataArray(0).trim.toLong, dataArray(1).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)
      .map(data => (DateUtils.getDateToString(data.timestamp).substring(0,13), 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger00())
      .aggregate(new TotalCount00, new TotalReWindow00)
      .map(one=>{
      println("key:"+one.key)
      val dateStream: String = DateUtils.getDateToString(one.window)

      println("time:"+dateStream)
      println("sum:"+one.sum)
    })

    env.execute("uv with bloom job")
  }
}

// 自定义窗口触发器
class MyTrigger00() extends Trigger[(String, Long), TimeWindow] {
  private val stateDescriptor = new ValueStateDescriptor[Integer]("total",classOf[Integer])

  override def onEventTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    val sumState: ValueState[Integer] = ctx.getPartitionedState(stateDescriptor)

    if (null == sumState.value)
      sumState.update(0)

    sumState.update(1 + sumState.value)
    if (sumState.value >= 2) { //这里可以选择手动处理状态
      //  默认的trigger发送是TriggerResult.FIRE 不会清除窗口数据
      System.out.println("触发.....数据条数为：" + (sumState.value))
      sumState.update(0)
      return TriggerResult.FIRE
    }else {
      println(element._1)
      TriggerResult.CONTINUE
    }
  }
}
//***************************************************************************************************************************************
//自定义聚合函数
class TotalCount00() extends AggregateFunction[(String,Long), Long,Long] {
  //累加操作
  override def add(value: (String,Long), accumulator: Long):Long = value._2+accumulator
  //初始状态
  override def createAccumulator(): Long = 0L
  //返回结果
  override def getResult(accumulator: Long): Long = accumulator
  //不同分区操作
  override def merge(a: Long, b: Long): Long = a+b
}

// 自定义窗口函数，包装成ItemViewCount输出
class TotalReWindow00() extends WindowFunction[Long, Rest, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[Rest]) {
    val ke = key
    val windowEnd = window.getEnd
    val tuple: Long = input.iterator.next()
    out.collect(Rest(ke, windowEnd, tuple))
  }
}
case class Rest(key:String,window:Long,sum:Long)