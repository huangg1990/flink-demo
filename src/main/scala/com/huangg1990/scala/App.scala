package com.huangg1990.scala

import com.huangg1990.java.entity.TestEntity

/**
 * java -cp target/flink-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
 * com.huangg1990.scala.App
 */
object App {

  def main(args: Array[String]): Unit = {

    val testEntity = new TestEntity()
    testEntity.setId("t1")
    testEntity.setNote("This is a test Entity!")
    testEntity.setCrate_time(System.currentTimeMillis())
    testEntity.setUpd_time(System.currentTimeMillis())

    print(testEntity.toString)
  }

}
