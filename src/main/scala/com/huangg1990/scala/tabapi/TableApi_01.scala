package com.huangg1990.scala.tabapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

object TableApi_01 {
  def main(args: Array[String]): Unit = {
    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val bsSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)

    val firstTableSQL =
      "CREATE TABLE t_person (\n  " +
        "`user_id` STRING,\n  " +
        "`user_name` STRING,\n  " +
        "`ts` BIGINT\n" +
        ") WITH (\n  " +
        "'connector' = 'kafka',\n  " +
        "'topic' = 'hg-person',\n  " +
        "'properties.bootstrap.servers' = '192.168.10.243:9092',\n  " +
        "'properties.group.id' = 'hgGroup',\n  " +
        "'scan.startup.mode' = 'earliest-offset',\n  " +
        "'format' = 'csv',\n" +
        "'csv.ignore-parse-errors' = 'true',\n " +
        "'csv.allow-comments' = 'true'\n " +
        ")"


    bsTableEnv.executeSql(firstTableSQL)


    //    val orders: Table = bsTableEnv.from("t_person")
    //    val result = orders
    //      .select($"user_id", $"user_name", $"ts")
    //    result.toAppendStream[(String, String, Long)].print("t1")

    val querqySQL = "select user_id,count(user_name) as ct from t_person group by user_id"
    //    val tableResult=bsTableEnv.sqlQuery(querqySQL).execute()
    //    tableResult.print()


    // execute SELECT statement
    val tableResult1 = bsTableEnv.executeSql(querqySQL)
    val it = tableResult1.collect()
    try while (it.hasNext) {
      val row = it.next
      println("user_id="+row.getField(0)+",ct="+ row.getField(1))
    }
    finally it.close() // close the iterator to avoid resource leak


    bsEnv.execute("table api test")


  }
}
