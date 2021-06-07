package com.rison.apitest

import java.lang
import java.lang.Thread

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

/**
 * @author : Rison 2021/6/7 上午10:00
 *
 */
case class MySensorSource() extends SourceFunction[SensorReading]{
  //定义一个标志位，flag,用来标示数据源是否正常运行发出数据
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //随机生成数据
    var currentMap: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => (
      "sensor_" + i, Random.nextDouble() * 100
    ))
    //定义一个循环，不停产生数据，除非调用cancel
    while (running){
      currentMap = currentMap.map(
        data => (data._1, data._2 + Random.nextGaussian())
      )
      //获取当前时间戳
      val currentTime = System.currentTimeMillis()
      currentMap.foreach(
        data => {
          sourceContext.collect(SensorReading(data._1, currentTime, data._2))
        }
      )

    }
  }

  override def cancel(): Unit = running = false
}
