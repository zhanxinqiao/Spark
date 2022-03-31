package com.zxq.Scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Test04_CountManPlus {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CountMan")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data: RDD[String] = sc.textFile("data/test.txt")
    val value: RDD[ListBuffer[String]] = data.map(x => {
      val lines = x.split(",")
      val strings: ListBuffer[String] = new ListBuffer[String]()   //①
      for (l <- lines) {
//        val strings: ListBuffer[String] = new ListBuffer[String]()   可变参数为数据类型的定类型变量  ①处用这句时  用以下变量添加值给strings
        strings.+=(l)
//        var strings: ListBuffer[String] = new ListBuffer[String]()   可变参数为数据类型的活类型变量  ①处用这句时  用以下变量添加值给strings
//        strings=strings:+l
      }
      strings
    })
    val data2: RDD[ListBuffer[String]] = value.filter(_ (3) == "男")
    data2.collect().foreach(println)
    println("男生人数为",data2.count())
  }
}
