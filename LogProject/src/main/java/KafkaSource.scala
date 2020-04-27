package source

import java.util.Properties

object KafkaSource {
  private val properties = new Properties()

  def  getProperties(address:String,groupid:String,officeSet:String)={
    properties.setProperty("bootstrap.servers", address)
    properties.setProperty("group.id", groupid)
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", officeSet)
    properties
  }
}
