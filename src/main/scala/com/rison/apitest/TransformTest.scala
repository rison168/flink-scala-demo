package com.rison.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/7 上午10:42
 *
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.readTextFile("data/sensor.txt")
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
//    reduceMap.print()

    //分流操作 split
    val splitStream: SplitStream[SensorReading] = streamMap.split(
      data => {
        if (data.temperature >= 35.6) Seq("high") else Seq("low")
      }
    )
    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
//    highStream.print("high")
//    lowStream.print("low")

    //合流操作 connect 数据类型可以不一致
    val warningStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))
    val connectStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowStream)
    // 用coMap对数据分别处理
    val coMapResultStream: DataStream[Product] = connectStreams.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
//    coMapResultStream.print()
    //联合 数据类型一致
    val unionStream: DataStream[SensorReading] = highStream.union(lowStream)
    unionStream.print()

    env.execute("transform test")
  }

}
