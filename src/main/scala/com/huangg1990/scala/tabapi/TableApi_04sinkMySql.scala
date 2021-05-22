package com.huangg1990.scala.tabapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7
 * docker exec -it mysql bash
 * mysql -uroot -p123456
 * grant all privileges on *.* to root@'%' identified by "password";
 * flush privileges;

-- ----------------------------
-- Table structure for temp_count
-- ----------------------------
DROP TABLE IF EXISTS `temp_count`;
CREATE TABLE `temp_count` (
  `id` varchar(64) NOT NULL,
  `cnt` bigint(10) DEFAULT NULL,
  `max_temp` double(10,2) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 *
 */
object TableApi_04sinkMySql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    val inputPath = "/Users/gwk/Downloads/huangg/gitroot/flink-demo/src/main/data/sensorreading.txt"
      //"/Users/tongzhou/test_code/flink-demo/src/main/data/sensorreading.txt"

    tableEnv.connect(new FileSystem().path(inputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("ts", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val DDLSql =
      """
        |create table mysql_outputTable (
        | id varchar(20) not null,
        | cnt bigint not null,
        | max_temp double null
        |) with (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://localhost:3306/hg_db',
        | 'connector.table' = 'temp_count',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'root',
        | 'connector.password' = '123456'
        |) """.stripMargin

    //registered table
    tableEnv.sqlUpdate(DDLSql);

    val querySql =
      """
        |select
        |id
        |,count(id) as cnt
        |,max(temp) as max_temp
        |from inputTable
        |group by id
        |""".stripMargin

    val resTable = tableEnv
      .sqlQuery(querySql)

    //print test
    //resTable.toRetractStream[(String, Long, Double)].print("res")

    //sink mysql
    //resTable.insertInto("mysql_outputTable")

    val explainAtion:String = tableEnv.explain(resTable)

    print(explainAtion)

    env.execute("table api sink mysql")
  }
}
