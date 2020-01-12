package flink

import bean.AllTotal
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kudu.client._
import org.slf4j.LoggerFactory

class MyFlinkKudu() extends RichSinkFunction[AllTotal]{
  private val logger = LoggerFactory.getLogger(classOf[MyFlinkKudu])

  //创建表名
  private val tableName = "HourData"
  //定义kudu主节点地址
  private val KUDU_MASTER = "node01:7051"
  //定义kudu表
  private var table: KuduTable = _
  //定义kudu客户端
  private var client: KuduClient = _
  //定义回话
  private var session: KuduSession = _

  override def open(parameters: Configuration) = {
    client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()
    session = client.newSession()
    table = client.openTable(tableName)
  }

  //  创建执行方法
  override def invoke(a: AllTotal, context: SinkFunction.Context[_]) {

    //获取处理得到的数据count，totoal
    a.windowEnd
    var id = 0;
    var count = a.total._1.toLong
    var total = a.total._2.toDouble
    println("count:" + count + "-------total:" + total)

    //更新数据
    val upsert: Upsert = table.newUpsert
    //获取行操作行对象
    val row1: PartialRow = upsert.getRow
    //将数据添加的row1对象中
    row1.addInt("id", id)
    row1.addLong("count", count)
    row1.addDouble("total", total)
    // 提交操作
    session.apply(upsert)
    session.flush()

  }

  override def close() {
    if (session != null) {
      session.close()
    }
    if (client != null) {
      client.close()
    }
    print("插入成功")

  }
}

