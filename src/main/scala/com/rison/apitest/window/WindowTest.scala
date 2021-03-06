package com.rison.apitest.window

import com.rison.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/6/8 上午8:49
 *
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从文件读取数据
    val stream: DataStream[String] = env.readTextFile("data/sensor.txt")
    //转换类型
    val streamMap: DataStream[SensorReading] = stream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    //TODO 每15秒统计一次，窗口内传感器的所有温度的最低值
    val dataStream: DataStream[(String, Double, Long)] = streamMap.map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1) //开窗之前一定要keyBy
      //      .window(TumblingEventTimeWindows.of(Time.seconds(5))) //滚动窗口
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) //滑动窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(10))) //会话窗口
      //      .timeWindow(Time.seconds(15)) //一个参数就是滚动窗口，2个参数是滑动窗口
      //       .countWindow(10)//同上
      .timeWindow(Time.seconds(15))
      .reduce(
        (curRes, newRes) => (curRes._1, curRes._2.min(newRes._2), curRes._3)
      )
    dataStream.print()

    env.execute("window_Func")
  }
}
