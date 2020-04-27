import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import source.KafkaSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}

object LogAnaylise {

  //生产上用这种方式处理日志
  private val log: Logger = LoggerFactory.getLogger("LogAnaylise")
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties: Properties = KafkaSource.getProperties("node01:9092", "project", "latest")
    val dateStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("example", new SimpleStringSchema(), properties))
    val value: DataStream[(String, String, String)] = dateStream.map(one => {
      val s: Array[String] = one.split("\t")
      val timeStr = s(3)
      var time = 0L
      try {
        time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeStr).getTime
      } catch {
        case e: Exception =>
          log.error(s"time parse error:$timeStr" + e.getMessage)
      }

      (s(2), time, s(5), s(6))
    }).filter(_._2 != 0L)
      .filter(_._1 == "E")
      .map(x => (x._2, x._3, x._4))
      //水位
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String)] {
      val maxOutOfOrderness = 10000L //10s
      var currentMaxTimestamp: Long = _ //scala里面的_非常重要
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, String), previousElementTimestamp: Long): Long = {
        val timestamp = element._1;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })
      //按照域名keyby
      .keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply(new WindowFunction[(Long, String, String), (String, String, String), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, String)], out: Collector[(String, String, String)]): Unit = {
          /**
            * 第一个参数：这一分钟的时间
            * 第二个参数：域名
            * 第三个参数：traffic的和
            */
          val domain: String = key.getField(0).toString
          var sum = 0L

          val i: Iterator[(Long, String, String)] = input.iterator
          while (i.hasNext) {
            val next: (Long, String, String) = i.next()
            sum += next._3.toLong //traffic求和
            //key拿到这个window的时间的next；

          }
          val time: String = new SimpleDateFormat("yyyy-MM-dd hh:mm").format(window.getStart)
          //            println("window:"+new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(window.getEnd))
          out.collect((time, domain, sum.toString))
        }
      })

    /**
      * 再生产上进行业务处理的时候，一定要考虑处理的健壮行以及数据的准确性，脏数据或者
      * 不符合业务规则的数据需要全部过滤滞后再进行业务逻辑处理
      *
      * 对于我们的业务来说，我们只需要统计level=E 的即可，对于level非E的，不作为我么业务指标的
      * 统计范畴
      *
      * 数据清洗：按照我们的业务跪着吧原始输入的数据进行一定规则的处理，使满足我们的业务需求为准
      */


    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.20.32", 9200))

    // 创建一个esSink 的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[(String,String,String)](
      httpHosts,
      new ElasticsearchSinkFunction[(String,String,String)] {
        override def process(element: (String,String,String), ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          println("saving data: " + element)
          // 包装成一个Map或者JsonObject
          val json = new util.HashMap[String, String]()
          json.put("time", element._1.toString)
          json.put("domain", element._2)
          json.put("traffics", element._3)

          var id = element._1+element._2
          // 创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("cdn")
            .`type`("traffic")
            .id(id)
            .source(json)

          // 利用index发送请求，写入数据
          indexer.add(indexRequest)
          println("data saved.")
        }
      }
    )


    value.addSink( esSinkBuilder.build() )

    env.execute()
  }
}
