import java.{lang, util}
import java.net.URL

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ListBuffer

//输出登录事件样例类
case class LoginEnvent(userId:Long,ip:String,eventType:String,eventTime:Long)
//输出异常报警信息样例类、
case class Warining(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)
object LoginFail {
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
    loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarningProcess(2))
      .print
    env.execute("login fail detect job")

  }
}
class LoginWarningProcess(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEnvent,Warining] {
  //定义状态保存2s内所有失败事件
  private val loginFailState: ListState[LoginEnvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEnvent]("login-fail-list",classOf[LoginEnvent]))

  override def processElement(value: LoginEnvent, ctx: KeyedProcessFunction[Long, LoginEnvent, Warining]#Context, out: Collector[Warining]): Unit ={
    val loginFailList: lang.Iterable[LoginEnvent] = loginFailState.get()

    //判断类型是否是fail，只添加fail的时间到状态
    if(value.eventType == "fail"){
      if(!loginFailList.iterator().hasNext){
        ctx.timerService().registerEventTimeTimer(value.eventTime*1000L +2000L)
      }
      loginFailState.add(value)
    }else{
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEnvent, Warining]#OnTimerContext, out: Collector[Warining]): Unit = {
    //触发定时器的时候，根据状态里的失败个数决定是否输出报警
    val allLoginFails = new ListBuffer[LoginEnvent]
    val iter: util.Iterator[LoginEnvent] = loginFailState.get().iterator()
    while(iter.hasNext){
      allLoginFails += iter.next()
    }

    //判断个数
    if(allLoginFails.length >= maxFailTimes){
      out.collect(Warining(allLoginFails.head.userId,allLoginFails.head.eventTime,allLoginFails.last.eventTime,"login falie "+ allLoginFails.length+" time."))
    }
    //清空状态
    loginFailState.clear()
  }

}