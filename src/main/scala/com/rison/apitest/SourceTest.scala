package com.rison.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumer09}

/**
 * @author : Rison 2021/6/7 上午9:16
 *
 */
//定义样例类,温度传感器

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1 定义集合数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )

    val stream1: DataStream[SensorReading] = env.fromCollection(dataList)
    stream1.print()

    //2 从文件读取数据
    val stream2: DataStream[String] = env.readTextFile("data/sensor.txt")
    stream2.print()

    //3 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    stream3.print()

    //4 自定义source
    val stream4: DataStream[SensorReading] = env.addSource(MySensorSource())
    stream4.print()


    //执行
    env.execute("source test")
  }

}
