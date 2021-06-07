package com.rison.apitest.sinktest

import java.util

import com.rison.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @author : Rison 2021/6/7 下午4:18
 *
 */
class MyElasticsearchSinkFunction extends ElasticsearchSinkFunction[SensorReading] {
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    //包装数据
    //定义data source
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id", t.id)
    dataSource.put("temperature", t.temperature.toString)
    dataSource.put("ts", t.timestamp.toString)

    //创建index request ,用于发送http请求
    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("_doc")
      .source(dataSource)

    //发送
    requestIndexer.add(indexRequest)
  }
}
