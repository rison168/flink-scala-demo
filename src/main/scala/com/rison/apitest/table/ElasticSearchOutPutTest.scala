package com.rison.apitest.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Elasticsearch, FileSystem, Json, OldCsv, Schema}

/**
 * @author : Rison 2021/6/10 下午3:57
 *
 */
object ElasticSearchOutPutTest {
  def main(args: Array[String]): Unit = {
    //1 、创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //2.1 读取文件
    val filePath = "data/sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING()).field("timestamp", DataTypes.BIGINT()).field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val inputTable = tableEnv.from("inputTable")
    val resultTable = inputTable.select('id,'temperature)
    val resultAggTable = inputTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    //输出到elasticSearch
    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("localhost", 9200, "http")
      .index("sensor")
      .documentType("_doc")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")
    resultAggTable.insertInto("esOutputTable")
    env.execute("es Test")
  }
}
