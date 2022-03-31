package com.zxq.Scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val value: RDD[(String, Int)] = sc.textFile("data/words.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).filter(_._2>3).filter(_._1!="")
    value.foreach(println)
  }

}
