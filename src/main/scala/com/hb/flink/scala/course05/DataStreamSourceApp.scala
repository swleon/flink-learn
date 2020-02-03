package com.hb.flink.scala.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * DataStream
 */
object DataStreamSourceApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;

    socketFunction(env) //从socket创建datastream

    env.execute("DataStreamSourceApp")
  }

  /**
   * 从socket创建datastream
   * @param env
   * @return
   */
  def socketFunction(env: StreamExecutionEnvironment) = {

    val data = env.socketTextStream("localhost",9999)

    data.print()
  }


}
