package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03_CouontMan {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CountMan")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data: RDD[String] = sc.textFile("data/test.txt")
    val l: Long = data.flatMap(_.split(",")).filter(_ == "男").count()
    println(l)
  }

}
