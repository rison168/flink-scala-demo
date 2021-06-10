package com.rison.apitest.table

import com.rison.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._


/**
 * @author : Rison 2021/6/10 上午10:10
 *
 */
object Example {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.readTextFile("data/sensor.txt")
    //转换类型
    val streamMap: DataStream[SensorReading] = stream.map(
      data => {
        val arr = data.split(" ")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

//    streamMap.print()
    //首先创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(streamMap)
    //调用tableApi进行转换
    val resultTable: Table = dataTable.select("id, temperature")
      .filter("id == 'sensor_1'")
    resultTable.toAppendStream[(String, Double)].print("result")

    //直接用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql = "select id, temperature from dataTable where id = 'sensor_1'"
    val resultSqlTable: Table = tableEnv.sqlQuery(sql)
    resultSqlTable.toAppendStream[(String, Double)].print("sqlResult")

    env.execute("tableAPI example")
  }
}
