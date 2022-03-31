package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test05_rdd_gaoji {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data: RDD[Int] = sc.parallelize(1 to 10, 5)
    println("----------------")
    //数据抽样 (数据开发必要环节) sample(withReplacement:是否允许把数据放回重抽，fraction：抽样数据的比例，seed：随机数种子)
    data.sample(true,0.1,222).foreach(println)
    println("----------------")
    data.sample(true,0.1,222).foreach(println)
    println("----------------")
    data.sample(false,0.1,221).foreach(println)
    println(s"data:${data.getNumPartitions}")

    val data1: RDD[(Int, Int)] = data.mapPartitionsWithIndex(
      //pt:分区迭代器  pi：分区索引
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )

    //将分区数量更改为三个  发现进行了一次shuffle 数据变为了一对多
    val repartitions: RDD[(Int, Int)] = data1.coalesce(3, false)
    val res: RDD[(Int, (Int, Int))] = repartitions.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )
    println("----------")
    println(s"data:${res.getNumPartitions}")
    data1.foreach(println)
    println("----------")
    res.foreach(println)
  }

}
