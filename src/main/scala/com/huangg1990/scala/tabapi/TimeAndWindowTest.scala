package com.huangg1990.scala.tabapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

case class SensorReading(id: String, timestamp: Long, temperature: Double)


object TimeAndWindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)


    val inputFilePath = //"/Users/tongzhou/test_code/flink-demo/src/main/data/sensorreading.txt"
      "/Users/gwk/Downloads/huangg/gitroot/flink-demo/src/main/data/sensorreading.txt"

    val inputStream = env.readTextFile(inputFilePath)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)


    tableEnv.connect(new FileSystem().path(inputFilePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
        //定义处理时间
        .field("pt", DataTypes.TIMESTAMP(3)).proctime()
      )
      .createTemporaryTable("inputTable")


    val sinkDDL:String =
      """
        |create table dataTable(
        |id varchar(20) not null,
        |ts bigint,
        |temperature double,
        |pt AS PROCTIME()
        |)with(
        |'connector.type' = 'filesystem',
        |'connector.path' = '/data/sensorreading.txt'
        |'format.type' = 'csv'
        |)
      """.stripMargin

    sensorTable.printSchema()

    sensorTable.toAppendStream[Row].print()


    env.execute("test proctime")

  }
}
