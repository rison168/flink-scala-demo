package com.rison.apitest.sinktest

import com.rison.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/7 下午2:49
 *
 */
object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataStream: DataStream[String] = env.readTextFile("data/sensor.txt")

    val dataMapStream: DataStream[SensorReading] = dataStream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0).toString, arr(1).toLong, arr(2).toDouble)
      }
    )

    dataMapStream.print()
//    dataMapStream.writeAsText("data/out.txt")
    dataMapStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("data/info.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("file sink")

  }
}
