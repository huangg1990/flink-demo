package com.huangg1990.scala.tabapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * docker pull wurstmeister/zookeeper
 * docker run -d --name zookeeper -p 2181:2181 -t wurstmeister/zookeeper
 *
 * docker run -d --name kafka -p 9092:9092 --link zookeeper  \
 * -e KAFKA_ZOOKEEPER_CONNECT=127.0.0.1:2181  \
 * -e KAFKA_ADVERTISED_HOST_NAME=127.0.0.1  \
 * -e KAFKA_ADVERTISED_PORT=9092  wurstmeister/kafka
 *
 */
object TableApi_05sinkKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build

    val tableEnv = StreamTableEnvironment.create(env,bsSettings)

    // bin/kafka-topics.sh --create --bootstrap-server 192.168.10.243:9092 --replication-factor 1 --partitions 1 --topic hg-input-topic
    // bin/kafka-topics.sh --zookeeper 192.168.10.243:2181 --list

    // bin/kafka-topics.sh --describe --topic hg-input-topic --bootstrap-server 192.168.10.243:9092

    // bin/kafka-console-producer.sh --bootstrap-server 192.168.10.243:9092 --topic hg-input-topic

    // bin/kafka-console-consumer.sh --topic hg-input-topic --from-beginning --bootstrap-server 192.168.10.243:9092

    //input kafkatable
    tableEnv.connect(
      new Kafka()
        .version("universal") //0.11
        .topic("hg-input-topic")
        .property("zookeeper.connect", "192.168.10.243:2181")
        .property("bootstrap.servers", "192.168.10.243:9092")
        .property("group.id", "hg-group1")
        .startFromEarliest()
    )
      .withFormat(
        new Csv()
          .ignoreParseErrors()
      )
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")


    //sink table
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("hg-out-topic")
      .property("zookeeper.connect", "192.168.10.243:2181")
      .property("bootstrap.servers", "192.168.10.243:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("cnt", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .inRetractMode()
      .createTemporaryTable("kafkaOutTable")


    val querySql =
      """
        |select
        |id
        |,count(id) as cnt
        |,max(temp) as max_temp
        |from inputTable
        |group by id
        |""".stripMargin

    val resTable = tableEnv.sqlQuery(querySql)
    resTable.toRetractStream[(String, Long, Double)].print("res")


    env.execute("sink kafka")
  }
}
