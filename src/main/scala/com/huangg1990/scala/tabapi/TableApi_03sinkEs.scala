package com.huangg1990.scala.tabapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv,Json, Elasticsearch, FileSystem, Schema}

// case class SensorReading(id: String, timestamp: Long, temperature: Double)

object TableApi_03sinkEs {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val bsSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    val filePath = "C:\\Users\\huangg\\Downloads\\gitroot\\flink-demo\\src\\main\\data\\sensorreading.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      ).createTemporaryTable("fileInputTable")


    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("192.168.199.250", 9200, "http")
      .index("sensor")
      .documentType("temperature")
    ).inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
        .field("max_temp", DataTypes.DOUBLE())
      ).createTemporaryTable("esOutputTable")


    val fileInputTable = tableEnv.from("fileInputTable")

    val aggTable = fileInputTable
      .orderBy($"id")
      .select($"id", $"id".count().as("count"), $"temp".max().as("max_temp"))


    aggTable.insertInto("esOutputTable")

    env.execute("sink es 6.* test")
  }

}
