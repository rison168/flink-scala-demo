package com.rison.apitest

import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/6/9 下午3:24
 *
 */
object SideOutPutTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend("hdfs:hadoop:8200"))
//    env.setStateBackend(new RocksDBStateBackend(""))
    //从文件读取数据
    val stream: DataStream[String] = env.readTextFile("data/sensor.txt")
    //转换类型
    val streamMap: DataStream[SensorReading] = stream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    val highTempStream: DataStream[SensorReading] = streamMap.process(SplitTempProcessor(30.0))
    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")
    env.execute("SideOutPutTest" )
  }
}

case class SplitTempProcessor(temperature: Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature > temperature){
      //如果当前温度大于30，输出到主流
      collector.collect(i)
    }else{
      //不超过30，输出到侧输出流
      context.output(new OutputTag[(String, Long, Double)]("late"), (i.id, i.timestamp, i.temperature))
    }
  }
}