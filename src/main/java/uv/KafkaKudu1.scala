package uv

import org.apache.kudu.client._

class KafkaKudu1(){
  def run(count:Long){

    var tableName="uv-day"
    var KUDU_MASTER="node01:7051"

    var  client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()
    var session = client.newSession()
    var table = client.openTable(tableName)

   //更新数据
    val upsert: Upsert = table.newUpsert
    //获取行操作行对象
    val row1: PartialRow = upsert.getRow
    //将数据添加的row1对象中
    row1.addInt("id", 0)
    row1.addLong("count", count)
    print("总数:"+count)
    // 提交操作
    session.apply(upsert)
    session.flush()

    if (session != null) {
      session.close()
    }
    if (client != null) {
      client.close()
    }
  }
}

