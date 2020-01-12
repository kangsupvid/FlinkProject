package Util

import java.nio.ByteBuffer
import java.util
import java.util.Date
import org.apache.flink.api.scala._
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableBiMap
import org.apache.kudu.client._
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.log4j.Logger
import bean.Student

class Util {
  var client:KuduClient = null
  var session:KuduSession= null
  val logger = Logger.getLogger(classOf[Util])

  def this(host: String) {
    this()
    this.client = new KuduClient.KuduClientBuilder(host).build
    if (client == null)
      throw new KuduClientException("ERROR: param \"host\" not valid, can't establish connection")
    this.session = this.client.newSession
  }
  private val TYPES = new ImmutableBiMap.Builder[Type, Class[_]]().
    put(Type.STRING, classOf[String]).
    put(Type.BOOL, classOf[Boolean]).
    put(Type.DOUBLE, classOf[Double]).
    put(Type.FLOAT, classOf[Float]).
    put(Type.BINARY, classOf[ByteBuffer]).
    put(Type.INT8, classOf[Byte]).
    put(Type.INT16, classOf[Short]).
    put(Type.INT32, classOf[Integer]).
    put(Type.INT64, classOf[Long]).
    put(Type.UNIXTIME_MICROS, classOf[Date]).build


  def getRowsPositionType(pos: Int, student: Student): Type = mapToType(student.productElement(pos).getClass)

  def mapToType(clazz: Class[_]): Type = TYPES.inverse.get(clazz)

//  def valueToRow(row: PartialRow, colType: Type, col: String, value: Any) = {
//    colType match {
//      case org.apache.kudu.Type.STRING =>
//        row.addString(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.BOOL =>
//        row.addBoolean(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.DOUBLE =>
//        row.addDouble(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.FLOAT =>
//        row.addFloat(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.BINARY =>
//        //row.addBinary(col, mapValue(value, mapFromType(colType)));
//
//      case org.apache.kudu.Type.INT8 =>
//        row.addByte(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.INT16 =>
//        row.addShort(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.INT32 =>
//        row.addInt(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.INT64 =>
//        row.addLong(col, mapValue(value, mapFromType(colType)))
//
//      case org.apache.kudu.Type.UNIXTIME_MICROS =>
//        row.addLong(col, mapValue(value, mapFromType(colType)))
//
//    }
//  }

  private def mapValue[T](value: Any, clazz: Class[_]) = value.asInstanceOf[T]

  def mapFromType(`type`: Type): Class[_] = TYPES.get(`type`)

  @throws[IllegalArgumentException]
  @throws[KuduException]
  def useTable(tableName: String, fieldsNames: Array[String], row: Student): KuduTable = {
    var table: KuduTable = null
    if (client.tableExists(tableName)) {
      logger.info("The table exists")
      table = client.openTable(tableName)
    }
    else if (tableName == null || tableName == "") throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableName\" parameter.")
    else if (fieldsNames == null || fieldsNames(0).isEmpty) throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Missing \"fields\" parameter.")
    else if (row == null) throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"row\" parameter.")
    else {
      logger.info("The table doesn't exist")
      table = createTable(tableName, fieldsNames, row)
    }
    table
  }


  @throws[KuduException]
  def createTable(tableName: String, fieldsNames: Array[String], row: Student): KuduTable = {
    if (client.tableExists(tableName)) return client.openTable(tableName)
    val columns: util.List[ColumnSchema] = new util.ArrayList[ColumnSchema]
    val rangeKeys: util.List[String] = new util.ArrayList[String] // Primary key
    rangeKeys.add(fieldsNames(0))
    logger.info("Creating the table \"" + tableName + "\"...")
    var i: Int = 0
    while ( {
      i < fieldsNames.length
    }) {
      var col: ColumnSchema = null
      val colName: String = fieldsNames(i)
      val colType: Type = getRowsPositionType(i, row)
      if (colName == fieldsNames(0)) {
        col = new ColumnSchema.ColumnSchemaBuilder(colName, colType).key(true).build
        columns.add(0, col) //To create the table, the key must be the first in the column list otherwise it will give a failure

      }
      else {
        col = new ColumnSchema.ColumnSchemaBuilder(colName, colType).build
        columns.add(col)
      }

      {
        i += 1; i - 1
      }
    }
    val schema: Schema = new Schema(columns)
    if (!client.tableExists(tableName)) client.createTable(tableName, schema, new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4))
    //logger.info("SUCCESS: The table has been created successfully");
    client.openTable(tableName)
  }

  def getClient: KuduClient = client





}
