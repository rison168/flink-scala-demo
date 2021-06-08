package com.rison.apitest.window

import com.rison.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/6/8 下午2:21
 *
 */
object WindowWaterMarkTest {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置事件语义
  env.getConfig.setAutoWatermarkInterval(500)

  //从文件读取数据
  val stream: DataStream[String] = env.readTextFile("data/sensor.txt")
  //转换类型
  val streamMap: DataStream[SensorReading] = stream.map(
    data => {
      val arr = data.split(" ")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }
  )
//    .assignAscendingTimestamps(_.timestamp * 1000L) //毫秒，升序数据，排好序的，不需要watermark
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(30)) {
      override def extractTimestamp(t: SensorReading) = t.timestamp * 1000L
    })// 一般给一个小的毫秒延时,比如三十毫秒,下面结合允许处理延迟数据、侧输出流保证数据
  streamMap.map( data => (data.id, data.timestamp, data.temperature))
    .keyBy(_._1)
    .timeWindow(Time.seconds(5))
    .allowedLateness(Time.minutes(1)) //最大迟到延时时间
    .sideOutputLateData(new OutputTag[(String, Long, Double)]("late")) //侧输出流，保证数据不丢



  env.execute("window_Func")
}
