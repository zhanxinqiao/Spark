package com.zxq.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test02_receiver02_custorm {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStream2").setMaster("local[9]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
     ssc.sparkContext.setLogLevel("ERROR")

    val dstream: ReceiverInputDStream[String] = ssc.receiverStream(new CustormReceiver("localhost", 8889))
    dstream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
