package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Test04_rdd_partitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("parititions")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data: RDD[Int] = sc.parallelize(1 to 10, 2)
    //外关联sql查询
    //方法一:每次都要打开连接-->查询-->关闭连接-->打印  IO过大
    val res01: RDD[String] = data.map(
      (value: Int) => {
        println("----conn--mysql------")
        println(s"---select$value------")
        println("----close--mysql--------")
        value + "selected"
      }
    )
    res01.foreach(println)

    println("----------------------------")
    //每个分区都建立一次连接  但是查询在内存堆压  关闭连接后才会输出  这里存在程序中断问题  会导致数据丢失
    val res02: RDD[String] = data.mapPartitionsWithIndex(
      (pindex, piter) => {
        val lb: ListBuffer[String] = new ListBuffer[String]//致命的问题   根据源码可发现 spark就是一个pipline,迭代器嵌套模式
        println(s"---$pindex---conn--mysql---")
        while (piter.hasNext) {
          val value: Int = piter.next()
          println(s"---$pindex--select$value----")
          lb.+=(value + "selectes")
        }
        println("-----close--mysql-------")
        lb.iterator
      }
    )
    res02.foreach(println)

    println("---------iterator----------")
    //重写迭代器  进入迭代器直接打开连接  如果还有数据  就进行查询打印 如果没有数据  则关闭连接  达到了实时同步的目的、
    val res03: RDD[String] = data.mapPartitionsWithIndex(
      (pindex, piter) => {
        //piter.flatMap()
        //piter.map()
        new Iterator[String] {
          println(s"---$pindex--conn--mysql----")

          override def hasNext = if (piter.hasNext == false) {
            println(s"---$pindex---close--mysql");
            false
          } else true

          override def next() = {
            val value: Int = piter.next()
            println(s"---$pindex--select $value----------")
            value + "selected"
          }
        }
      }
    )
    res03.foreach(println)
  }
}
