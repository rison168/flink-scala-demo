package com.rison.apitest.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

/**
 * @author : Rison 2021/6/10 上午10:49
 *
 */
object TableApiTest {

  def main(args: Array[String]): Unit = {
    //1 、创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //1.1 基于老版本planner的流处理
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //1.2 基于老版本的批处理
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    //1.3 基于blink planner的批处理
    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val blinkStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings)

    //1.4 基于blink 批处理
    val blinkBatchTableSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val blinkBatchTableEnv: TableEnvironment = TableEnvironment.create(blinkBatchTableSettings)

    //2 连接外部系统，读取数据，注册表

    //2.1 读取文件
    val filePath = "data/sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING()).field("timestamp", DataTypes.BIGINT()).field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val inputTable = tableEnv.from("inputTable")

//    inputTable.toAppendStream[(String, Long, Double)].print()

    //2.2 从kaFka读取数据
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9200")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING()).field("timestamp", DataTypes.BIGINT()).field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaInputTable")
    val kafkaInputTable = tableEnv.from("kafkaInputTable")
    kafkaInputTable.toAppendStream[(String, Long, Double)].print()


    // 3 查询转换
    //3.1  使用table api
    val sensorTable: Table = tableEnv.from("inputTable")
    val resultSqlTable: Table = sensorTable.select('id, 'temperature).filter('id === "sensor_1")
    //3.2 SQL
    val sqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id ='sensor_1'
        |""".stripMargin)
    sqlTable.toAppendStream[(String, Long, Double)].print()
    resultSqlTable.toAppendStream[(String, Long, Double)].print()

    env.execute("Table API Test")
  }

}
