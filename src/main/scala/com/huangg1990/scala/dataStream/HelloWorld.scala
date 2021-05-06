package com.huangg1990.scala.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream: DataStream[String] = env.socketTextStream("127.0.0.1", 7777)

    val resDataStream = dataStream
      .flatMap(l => {
        l.split(" ")
      })
      .map(w => (w, 1))
      .keyBy(_._1)
      .sum(1)
    resDataStream.print("res")


    env.execute("hello world")
  }
}
