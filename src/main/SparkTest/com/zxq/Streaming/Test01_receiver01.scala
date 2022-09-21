package com.zxq.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test01_receiver01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStream").setMaster("local[9]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)

//    val flatDStream: DStream[String] = dataDStream.flatMap(_.split(" "))
//    val resDStream: DStream[(String, Int)] = flatDStream.map((_, 1)).reduceByKey(_ + _)
//    resDStream.print()

    val resDStream: DStream[(String, String)] = dataDStream.map(_.split(" ")).map(vars => {
      Thread.sleep(20000)
      (vars(0), vars(1))
    })
    resDStream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
