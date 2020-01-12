case class KafkaInfo (`data`:String,
                      database:String,
                      es:String,
                      id:String,
                      isDdl:String,
                      mysqlType:String,
                      old:String,
                      pkNames:String,
                      sql:String,
                      sqlType:String,
                      table:String,
                      ts:String,
                      `type`:String
                     )
case class MysqlInfo(uid:String,
                     pid:String,
                     position:String,
                     price:Double,
                     timestamp:String)
//case class Infor(count:Long,total:Double)
case class Infor(count:Long,total:Double)

case class My(uid:String,
               pid:String,
               position:String,
               price:String,
               timestamp:String)


case class AllTotal(key:String,windowEnd:Long,total:(Long,Double))
case class UvCount(window:Long,uvCount:Long)
case class Information(id:Int,count:Long,total:Double)