package com.rison.apitest.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @author : Rison 2021/6/10 下午3:44
 *
 */
object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {
    //1 、创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

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
    //查询转换
    val kafkaInputTable = tableEnv.from("kafkaInputTable")
    //简单转换
    val resultTable = kafkaInputTable.select('id,'temperature)
    //聚合转换
    val resultAggTable = kafkaInputTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    //输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sink_sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9200")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING()).field("timestamp", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutPutTable")

    resultAggTable.insertInto("kafkaOutPutTable")
    env.execute("kafka pipeline")
  }

}
