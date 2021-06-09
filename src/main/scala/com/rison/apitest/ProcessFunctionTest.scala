package com.rison.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/6/9 下午2:23
 *
 */
object ProcessFunctionTest {
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

    val warningStream: DataStream[String] = streamMap.keyBy(_.id)
      //      .timeWindow(Time.seconds(10))
      .process(new TempIncreWarning(10000L))
      warningStream.print()

    env.execute("ProcessFunctionTest")
  }



}
//keyedProcessFunction功能测试
class MyProcessFunction extends KeyedProcessFunction[String, SensorReading, String]{
  lazy val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))
  override def open(parameters: Configuration): Unit = {

  }


  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    context.getCurrentKey
    context.timestamp()
    context.output(new OutputTag[SensorReading]("late"), i)
    context.timerService().currentWatermark()
    context.timerService().registerEventTimeTimer(context.timestamp() + 60000L)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
}

case class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String]{
//定义状态，保存上一个温度值进行比较，保存注册定时器的时间戳用于删除
  lazy val lastTimeState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTimeState", classOf[Double]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTsState", classOf[Long]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    val lastTimeTemp = lastTimeState.value()
    val timerTs = timerTsState.value()

    //当前温度值和上次进行比较
    if (i.temperature > lastTimeTemp && timerTs == 0) {
      //如果温度上述，且没有定时器，，那么注册当前时间10s之后的定时器
      val ts: Long = context.timerService().currentProcessingTime() + interval
      context.timerService().registerProcessingTimeTimer(ts)
      timerTsState.update(ts)
    }
      //温度下降删除定时器
    else if(i.temperature < lastTimeTemp){
      context.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear()
    }
    lastTimeState.update(i.temperature)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval / 1000 + "秒持续上升！")
    timerTsState.clear()
  }
}