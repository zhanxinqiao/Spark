package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03_rdd_aggregator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val context: SparkContext = new SparkContext(conf)
    context.setLogLevel("ERROR")
    val data: RDD[(String, Int)] = context.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 5667),
      ("zhangsan", 343),
      ("lisi", 212),
      ("lisi", 44),
      ("lisi", 33),
      ("wangwu", 535),
      ("wangwu", 22)
    ))
    //按Key Value --> 一组
    println("------按字典格式输出----------")

    val group: RDD[(String, Iterable[Int])] = data.groupByKey()
    group.foreach(println)

    println("--------行列转换输出---------")
    println("--------①---------")
    //行列转换
    //flatMap-->压缩    e._2.map-->取字典值CompactBuffer(234, 5667, 343) 进行迭代
    // x => (e._1, x)-->将最先的key放入元组第一个元素   迭代的值进入元组的第二个元素
    val res01: RDD[(String, Int)] = group.flatMap(e => e._2.map(x => (e._1, x)).iterator)
    res01.foreach(println)

    println("-------②----------")
    //flatMapValues(e=>e.iterator) 将值进行压缩  然后取出值进行迭代(迭代方式是(key，value)的元组样式)
    group.flatMapValues(e=>e.iterator).foreach(println)

    println("--------③---------")
    //mapValues:将group中的值拿出来进行遍历  e=>e.toList.sorted.take(2):取出值转换为List数组，再排序，再取出前两名
    group.mapValues(e=>e.toList.sorted.take(2)).foreach(println)

    println("-------④----------")
    //flatMapValues：将值进行压缩 () 然后产生新的（Key,Value）的映射  最后只取前两个
    group.flatMapValues(e=>e.toList.sorted.take(2)).foreach(println)

    println("--------sum,count,min,max,avg------------------")
    println("------sum-----")
    val sum: RDD[(String, Int)] = data.reduceByKey(_ + _)
    sum.foreach(println)
    println("------max-----")
    val max: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov > nv) ov else nv)
    max.foreach(println)
    println("------min-----")
    val min: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov < nv) ov else nv)
    min.foreach(println)
    println("------count-----")
    val count: RDD[(String, Int)] = data.mapValues(e => 1).reduceByKey(_ + _)
    count.foreach(println)
    println("------avg-----")
    //sum算子连接count算子后，相同的key为一组，(sum,count)为数据元组
    val tmp: RDD[(String, (Int, Int))] = sum.join(count)
    val avg: RDD[(String, Int)] = tmp.mapValues(e => e._1 / e._2)
    avg.foreach(println)

    println("------avg-----combine方法------")
    val tmpx: RDD[(String, (Int, Int))] = data.combineByKey(
      //createCombiner:V=>C,
      //第一条记录的value怎么放hashmap
      (value: Int) => (value, 1),
      //mergeValue:(C,V)=>C
      //如果有第二条记录，第二条以及以后的他们的value怎么放到hashmap里：
      (oldValue: (Int, Int), newvalue: Int) => (oldValue._1 + newvalue, oldValue._2 + 1),
      //mergeCombiners:(C:C)=>C
      //合并溢写结果的函数
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)
    )
    tmpx.mapValues(e=>e._1/e._2).foreach(println)
  }

}
