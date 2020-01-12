package test

import org.apache.kudu.client._

class K1() {

  def run(id:Int,count:Int){
    var tableName="abc"
    var KUDU_MASTER="node01"
    println("-----------------------------------------------")

    var  client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()
    var session = client.newSession()
    var table = client.openTable(tableName)

    //    println("count:" + id + "-------total:" + count)

    //更新数据
    val upsert: Upsert = table.newUpsert
    //获取行操作行对象
    val row1: PartialRow = upsert.getRow
    //将数据添加的row1对象中
    row1.addInt("id", id)
    row1.addInt("count", count)

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

