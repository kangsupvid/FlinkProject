import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector


import scala.collection.mutable.ListBuffer



case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间处理类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //处理数据
//    val dataStream: DataStream[UserBehavior] = env.readTextFile("C:\\Users\\kang\\Desktop\\最新复习\\文档\\FlinkProject\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val properties: Properties = KafkaSource.getProperties("node01:9092", "groupid", "latest")
    val dateStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("kafka-uv", new SimpleStringSchema(), properties))
    dateStream .map(one => {
        val data: Array[String] = one.split(",")
        UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
      })
        .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior == "pv")
        .keyBy(_.itemId)
        .timeWindow(Time.minutes(60),Time.minutes(5))
        .aggregate(new CountAgg(),new WindowResultFunction())
        .keyBy(_.windowEnd)
        .process(new ToNHotItems(3))
        .print("hotItems")



  env.execute("Hote Items Job")
  }
}

// COUNT统计的聚合函数实现，每出现一条记录就加一
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  //初始化
  override def createAccumulator(): Long = 0L
  //聚合逻辑
  override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1
  //得到结果
  override def getResult(acc: Long): Long = acc
  //分区聚合
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2

}
//输出窗口结果
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, aggregateResult: Iterable[Long],
                     collector: Collector[ItemViewCount]) : Unit = {
    val itemId: Long = key
    val count = aggregateResult.iterator.next
    collector.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}
// 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
class ToNHotItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  //定义一个list State状态，用来保存所有的ItemViewCount的状态
  private var itemStata : ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemStata = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long,ItemViewCount, String]#Context,
                              out: Collector[String]){
    //每一条数据都存入state中
    itemStata.add(value)
    //注册定时器，延迟处罚；当定时器触发时，当前windowEnd的一组数据应该都到齐，统一排序处理
    ctx.timerService().registerEventTimeTimer(value.windowEnd)

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]){
    //定时器触发时，已经收集到所有数据，首先把数据放到一个list中
    val allItems : ListBuffer[ItemViewCount] = new ListBuffer
    import scala.collection.JavaConversions._
    for(item <- itemStata.get){
      allItems += item
    }
    itemStata.clear()

    //按照count大小进行排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //将数据排名信息格式化String，方便打印输出
    val result : StringBuilder = new StringBuilder()
    result.append("========================================\n")
    result.append("时间： ").append(new Timestamp(timestamp)).append("\n")
    //每一个商品信息输出
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(": ")
        .append("商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("========================================\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}