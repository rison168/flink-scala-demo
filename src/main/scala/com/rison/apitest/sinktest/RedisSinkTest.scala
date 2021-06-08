package com.rison.apitest.sinktest

import java.util.Properties

import com.rison.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author : Rison 2021/6/7 下午3:37
 *
 */
object RedisSinkTest {
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
    val conf =  new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()
   dataMapStream.addSink(new RedisSink[SensorReading](conf, new MyRedisSinkMapper))
    env.execute("redis sink")
  }
}


class MyRedisSinkMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    //定义保存数据到redis命令, hset 表名 key value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }
  //将id指定为key
  override def getKeyFromData(t: SensorReading): String = {
    t.id.toString
  }

  //将温度值指定为value
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}