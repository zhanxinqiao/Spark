package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//单词统计
object Test0_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context: SparkContext = new SparkContext(conf)
    context.setLogLevel("ERROR")
    //方法一：
    //DATASET
    val value: RDD[String] = context.textFile("data/testdata.txt", 16)
    value.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    println("----------------------------------")
    //方法二：
    val fileRDD: RDD[String] = context.textFile("data/testdata.txt")
    val words: RDD[String] = fileRDD.flatMap((x: String) => {
      x.split(" ")
    })
    val pairWord: RDD[(String, Int)] = words.map((x: String) => {
      new Tuple2(x, 1)
    })
    val res: RDD[(String, Int)] = pairWord.reduceByKey((x, y) => {
      x + y
    })
    res.foreach(println)
//    while (true){
//
//    }
  }
}
