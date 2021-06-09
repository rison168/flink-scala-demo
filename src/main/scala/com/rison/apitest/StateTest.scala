package com.rison.apitest

import java.util

import com.rison.apitest.window.WindowWaterMarkTest.env
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/6/9 上午9:25
 *
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从文件读取数据
    val stream: DataStream[String] = env.readTextFile("data/sensor.txt")
    //转换类型
    val streamMap: DataStream[SensorReading] = stream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    //需求： 对于温度传感器温度值跳变，超过10度，报警

    val alterStream = streamMap.keyBy(_.id)
      //      .flatMap(
      //        TempChangeAlert(10.0)
      //      )
      .flatMapWithState[(String, Double, Double), Double]{
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp: Some[Double]) => {
          //比较
          val temp: Double = (data.temperature - lastTemp.get).abs
          if (temp > 10.0) {
            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
          } else {
            (List.empty, Some(data.temperature))
          }
        }
      }
    alterStream.print()

    env.execute("state Test")

  }
}

//keyed state : 必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading, String] {
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduceState", (data1, data2) => new SensorReading(data2.id, data2.timestamp, data1.temperature.min(data2.temperature)), classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  }

  override def map(in: SensorReading): String = {
    //状态的读写
    //valueState
    val myValueState = valueState.value()
    valueState.update(in.temperature)
    //listState
    listState.add(1)
    val ints = new util.ArrayList[Int]()
    ints.add(2)
    ints.add(3)
    listState.addAll(ints)
    listState.update(ints)
    //mapState
    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    //reducingState
    reduceState.get()
    reduceState.add(in)

    in.id
  }


}