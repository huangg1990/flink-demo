package com.huangg1990.scala.tabapi


import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, Schema}


/**
  * http://127.0.0.1:9200/hg-test/_search
  * 查看数据
  */
object TableApi_03sinkES2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    val inputFilePath = "/Users/gwk/Downloads/huangg/gitroot/flink-demo/src/main/data/sensorreading.txt"

    tableEnv.connect(new FileSystem().path(inputFilePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("tm", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("127.0.0.1", 9200, "http")
        .index("hg-test")
        .documentType("testType") //es 6 必须
        .bulkFlushMaxActions(1) //最小批处理条数
    ).withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("cnt", DataTypes.BIGINT())
        .field("max_tmp", DataTypes.DOUBLE())
      )
      .inUpsertMode() // 写入模式
      .createTemporaryTable("esOutputTable")


    val inputTable: Table = tableEnv.from("inputTable")

    val resTable = inputTable
      .groupBy('id)
      .select('id, 'id.count as 'count, 'temp.max as 'max_tmp)

    resTable.insertInto("esOutputTable")

    resTable.toRetractStream[(String, Long, Double)].print("res")

    env.execute("sink es test")

  }
}
