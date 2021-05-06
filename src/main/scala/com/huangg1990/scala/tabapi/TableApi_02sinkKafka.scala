//package com.huangg1990.scala.tabapi
//
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.bridge.scala._
//import org.apache.flink.table.api._
//
//object TableApi_02sinkKafka {
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    val bsSettings = EnvironmentSettings
//      .newInstance()
//      .useBlinkPlanner()
//      .inStreamingMode()
//      .build()
//    // create a TableEnvironment for specific planner batch or streaming
//    val tableEnv = StreamTableEnvironment.create(env, bsSettings)
//
//    val inputPersonTableSql =
//      """
//        |CREATE TABLE t_person (
//        |  `user_id` STRING,
//        |  `user_name` STRING,
//        |  `ts` BIGINT
//        |) WITH (
//        |  'connector' = 'kafka',
//        |  'topic' = 'hg-person',
//        |  'properties.bootstrap.servers' = '192.168.10.241:9092;192.168.10.242:9092;192.168.10.243:9092',
//        |  'properties.group.id' = 'hgGroup',
//        |  'scan.startup.mode' = 'earliest-offset',
//        |  'format' = 'csv',
//        |  'csv.ignore-parse-errors' = 'true',
//        |  'csv.allow-comments' = 'true'
//        | )
//        |""".stripMargin;
//
//    val outputTableSql =
//      """
//        |CREATE TABLE t_person_out (
//        |  `user_id` STRING,
//        |  `ct` BIGINT,
//        |  PRIMARY KEY (`user_id`) NOT ENFORCED
//        |) WITH (
//        |  'connector' = 'upsert-kafka',
//        |  'topic' = 'hg-person-out',
//        |  'properties.bootstrap.servers' = '192.168.10.241:9092;192.168.10.242:9092;192.168.10.243:9092',
//        |  'key.format' = 'avro',
//        |  'value.format' = 'csv'
//        | )
//        |""".stripMargin;
//    /**
//     * avro
//     * csv
//     * raw
//     */
//
//    //    'key.format' = 'avro',
////    'value.format' = 'avro'
//
//    // create an input Table
//    tableEnv.executeSql(inputPersonTableSql)
//    // register an output Table
//    tableEnv.executeSql(outputTableSql)
//
//    // create a Table from a Table API query
//    //    val table2 = tableEnv.from("t_person")
//    //      .filter($("user_id").like("uid_%"))
//    //      .groupBy($"user_id")
//    //      .select($("user_id"), $("user_id").count().as("ct"))
//    //    //table2.toAppendStream[(String, String, Long)].print("rs")
//    //    table2.toRetractStream[(String, Long)].print("agg")
//
//    val aggsql =
//      """
//        |select user_id,count(user_id) as ct
//        |from t_person
//        |where user_id like 'uid_%'
//        |group by user_id
//        |""".stripMargin
//    // create a Table from a SQL query
//    val table3 = tableEnv.sqlQuery(aggsql)
//
//    table3.toRetractStream[(String, Long)].print("agg")
//
//    // emit a Table API result Table to a TableSink, same for SQL result
//    val tableResult: TableResult = table3.executeInsert("t_person_out")
//
//
//    env.execute("sink test")
//
//
//  }
//}
