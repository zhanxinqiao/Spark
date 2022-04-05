package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test05_PVUV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("PVUV")
//    val conf: SparkConf = new SparkConf().setAppName("PVUV")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //对网址 以及用户ID去重并统计
        val file: RDD[String] = sc.textFile("data/jc_content_viewlog.txt")
//    val file: RDD[String] = sc.textFile("/tem/spark/jc_content_viewlog.txt")
    val value: RDD[(String, String, String)] = file.map(line => {
      val strings: Array[String] = line.split(",")
      //ID(序号):478896,网页ID:1043,网址:/jszx/1043.jhtml,用户ID:14884,缓存生成ID:F6D362B9AFAC436D153B7084EF3BA332,时间：2017-03-01 00:23:07
      //通过观察 网页ID 和 网址是一一对应关系 统计被访问网址个数 二者取其一 这里选择网页ID
      (strings(1), strings(3), strings(5)) //这里将接下来要用的 网址和用户ID 时间都取出来
    })

    //对网页去重并统计
    val Count_Page_path: Long = value.map(x => {
      val distinct_Page_path: String = x._1.distinct
      distinct_Page_path
    }).count()
    println("对网址去重并统计:",Count_Page_path)


    //对用户去重并统计
    val Count_Userid: Long = value.map(x => {
      val distinct_Userid: String = x._2.distinct
      distinct_Userid
    }).count()
    println("对用户去重并统计:",Count_Userid)

    //按月统计访问数量
    val monthpartition: RDD[(String, Int)] = value.map(x => {
      val month: String = x._3.split("-")(1)
      (month, 1)
    })
    val monthCount: RDD[(String, Int)] = monthpartition.reduceByKey(_ + _, 12)
    println("按月统计访问数量:")
    monthCount.foreach(println)
  }
}
