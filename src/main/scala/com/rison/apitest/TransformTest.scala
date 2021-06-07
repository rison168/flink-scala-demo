package com.rison.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/7 上午10:42
 *
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.readTextFile("data/sensor.txt").setParallelism(1)
    //转换类型
    val streamMap: DataStream[SensorReading] = stream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    //分组聚合
    val aggStream: DataStream[SensorReading] = streamMap.keyBy("id").minBy("temperature")
//    aggStream.print()
    println("+++++++++++++++++++++++++++")
    val reduceMap: DataStream[SensorReading] = streamMap.keyBy("id").reduce(
      (curState, newData) => {
        SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
      }
    )
    reduceMap.print()
    env.execute("transform test")
  }

}
