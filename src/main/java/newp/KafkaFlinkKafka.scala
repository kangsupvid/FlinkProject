package newp

import java.util.Properties

import bean._
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import source.KafkaSource

object KafkaFlinkKafka  {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties: Properties = KafkaSource.getProperties("node01:9092", "groupid", "latest")
    val dateStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("example", new SimpleStringSchema(), properties))

    //    env.enableCheckpointing(5000)//5秒一个checkpoint
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//指定处理的时间特性
    //    env.setRestartStrategy(RestartStrategies.noRestart())//重启策略
    //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义
    //
    ////   val checkPointPath = new Path("hdfs:///user/root/flink/checkpoint")//fs状态后端配置,如为file:///,则在taskmanager的本地
    //   val checkPointPath = new Path("hdfs://node01:9000/user/root/flink/checkpoint")//fs状态后端配置,如为file:///,则在taskmanager的本地
    ////    val checkPointPath = new Path("file:///opt/checkpoint")//fs状态后端配置,如为file:///,则在taskmanager的本地
    //    val fsStateBackend: StateBackend= new FsStateBackend(checkPointPath)
    ////    env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/");
    //    env.setStateBackend(fsStateBackend)
    //    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint

    //    利用fastJson工具，将接收到的数据转换成kafkaInfo类型的数据
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


    val value: DataStream[String] = mysqlInfoDate
      //利用map算子，传入一个字符串“all”，获取price，得到一个二元组
      .map(one => ("all", one.price))
      //按照第一个元素进行分组
      .keyBy(_._1)
      //设置滑动窗口参数，窗口长度1天，滑动间距2s
      .timeWindow(Time.days(1), Time.seconds(5))
      //实现数据的聚合，自定义累加器
      .aggregate(new TotalCount1, new TotalReWindow1)
      .map(one => {
        val infor = one.total._1+"-"+one.total._2
        new Producer().run(infor)
        infor
      })

      value.addSink(new FlinkKafkaProducer011[String]("node01:9092", "kafka-kudu", new SimpleStringSchema()))

      env.execute()
  }
}
//***************************************************************************************************************************************
//自定义聚合函数
class TotalCount1() extends AggregateFunction[(String,Double), (Long,Double),(Long,Double)] {
  //累加操作
  override def add(value: (String,Double), accumulator: (Long,Double)):(Long,Double) = (accumulator._1+1,accumulator._2+value._2)
  //初始状态
  override def createAccumulator(): (Long,Double) = (0L,0.0)
  //返回结果
  override def getResult(accumulator: (Long,Double)): (Long,Double) = accumulator
  //不同分区操作
  override def merge(a: (Long,Double), b: (Long,Double)): (Long,Double) = (a._1+b._1, a._2 + b._2)
}

// 自定义窗口函数，包装成ItemViewCount输出
class TotalReWindow1() extends WindowFunction[(Long,Double), AllTotal, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[(Long, Double)], out: Collector[AllTotal]) {
    val ke = key
    val windowEnd = window.getEnd
    val tuple: (Long, Double) = input.iterator.next()
    out.collect(AllTotal(ke, windowEnd, tuple))
  }
}


