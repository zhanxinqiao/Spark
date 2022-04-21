package com.zxq.Streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test0015_blackList {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("BlackList")

    val ssc = new StreamingContext(conf, Seconds(5))

    val sc: SparkContext = ssc.sparkContext
    sc.setLogLevel("ERROR")
    val value: RDD[String] = sc.textFile("/tem/SparkWork/15/mingdan.txt")
    val value1: RDD[(String, String)] = value.map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(0), strings(1))
    })

    val sots: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 8888)
//    val sots: ReceiverInputDStream[String] = ssc.socketTextStream("local", 8888)
    val value2: DStream[(String, String)] = sots.map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(1), strings(0))
    })


    val value4: DStream[(String, String)] = value2.transform(value2Rdd => {
      val value3: RDD[(String, String)] = value1.leftOuterJoin(value2Rdd)
        .filter(x => {
          if (x._2._1 == "false") {
            false
          } else {
            true
          }
        }).map(x => (x._1, x._2._2.toString))
      value3.filter(x=>x._2!="None")
    })

    value4.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
