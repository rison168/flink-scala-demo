package com.rison.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/6/9 上午10:33
 *
 */
case class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  //定义状态 保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]))
  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上一次的温度值
    val lastTemp = lastTempState.value()
    //比较
    val temp: Double = (in.temperature - lastTemp).abs
    if (temp > threshold){
      collector.collect(in.id, lastTemp, in.temperature)
      //更新状态
      lastTempState.update(in.temperature)
    }
  }
}
