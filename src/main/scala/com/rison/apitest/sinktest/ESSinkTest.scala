package com.rison.apitest.sinktest

import java.util
import java.util.Properties

import com.rison.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost

/**
 * @author : Rison 2021/6/7 下午4:03
 *
 */
object ESSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //3 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    val dataMapStream: DataStream[SensorReading] = dataStream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0).toString, arr(1).toLong, arr(2).toDouble)
      }
    )
    import scala.collection.JavaConverters._
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    val esSinkFunc = new ElasticsearchSink.Builder[SensorReading](httpHosts, new MyElasticsearchSinkFunction).build()
    dataMapStream.addSink(esSinkFunc)
    env.execute("elasticsearch_sink")
  }
}
