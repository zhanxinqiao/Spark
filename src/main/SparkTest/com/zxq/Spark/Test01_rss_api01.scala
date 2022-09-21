package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//

object Test01_rss_api01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Api01")
    val context: SparkContext = new SparkContext(conf)
      context.setLogLevel("ERROR")
    val dataRDD: RDD[Int] = context.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))
    //1.过滤
    val value: RDD[Int] = dataRDD.filter(_ > 3)
    val res01: Array[Int] = dataRDD.collect() //返回一个数组，因为数组元素都会放在内存，故数组不可太大
    res01.foreach(println)
    println("----------------------")
    //用map去重
    val res: RDD[Int] = dataRDD.map((_, 1)).reduceByKey(_ + _).map(_._1)
    res.foreach(println)
    //用算子的distinct方法去重
    println("----------------------")
    val resx: RDD[Int] = dataRDD.distinct()
    resx.foreach(println)
    /*
    * 面向数据集开发  面向数据集API 1，基础API  2,复合API
    * RDD (HadoopRDD,MappartitionsRDD,ShuffledRDD...)
    * map,flatmap,filter
    * distinct
    *
    * reduceByKey: 复合 ->combineByKey()
    * */

    //面向数据集:交并差  关联  笛卡尔积
    //面向数据集:元素-->单元素，K,V元素-->结构化，非结构化
    val rdd1: RDD[Int] = context.parallelize(List(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = context.parallelize(List(3, 4, 5, 6, 7))
    //差集
    println("----------差集----subtract----")
    val subtract: RDD[Int] = rdd1.subtract(rdd2)
    subtract.foreach(println)
    println("----------交集----intersection----")
    val intersection: RDD[Int] = rdd1.intersection(rdd2)
    intersection.foreach(println)
    //如果数据不需要区分每一条记录归属与那个分区......    间接的，这样的数据不需要partitioner......  不需要shuffle
    //因为shuffle的语义:洗牌--> 面向每一条记录计算它的分区号
    //如果有行为:不需要区分记录，本地IO拉取数据   那么这种直接IO一定比先partition-->计算-->shuffle落入文件，最后在IO拉取速度快
    println("----------笛卡尔积集----cartesian----")
    val cartesian: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    cartesian.foreach(println)
    println("----------并集----union----")
    println(rdd1.partitions.size)
    println(rdd2.partitions.size)
    val unitRDD: RDD[Int] = rdd1.union(rdd2)
    println(unitRDD.partitions.size)
//    unitRDD.map(sddf)  可用并集进行相应的API调用  达到操作数据集的目的

    val kv1: RDD[(String, Int)] = context.parallelize(List(
      ("zhansan", 11),
      ("zhansan", 12),
      ("lisi", 13),
      ("wangwu", 14)
    ))
    val kv2: RDD[(String, Int)] = context.parallelize(List(
      ("zhansan", 21),
      ("zhansan", 22),
      ("lisi", 23),
      ("zhaoliu", 28)
    ))
    println("------分组(相同K的为一组，并将依据其不同的RDD组 将其放在不同的缓冲组中)------------")
    //(zhansan,(CompactBuffer(11, 12),CompactBuffer(21, 22)))

    val cogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = kv1.cogroup(kv2)
    cogroup.foreach(println)

    println("-------连接(即将两个RDD内相同的Key所连接的缓冲组数据做笛卡尔积，不同的Key舍弃)------------")
    val join: RDD[(String, (Int, Int))] = kv1.join(kv2)
    join.foreach(println)

    println("-----左连接(相同的Key相连，做一对多的映射，左有右无(wangwu,(14,None))，左无右有不映射)--------------")
    val left: RDD[(String, (Int, Option[Int]))] = kv1.leftOuterJoin(kv2)
    left.foreach(println)

    println("-----右连接(相同的Key相连，做一对多的映射，右有左无(zhaoliu,(None,28))，左有右无不映射)--------------")
    val right: RDD[(String, (Option[Int], Int))] = kv1.rightOuterJoin(kv2)
    right.foreach(println)

    println("------全连接(全映射)，无论  左无右有  还是  右无左有 全部显示--------------------------")
    val full: RDD[(String, (Option[Int], Option[Int]))] = kv1.fullOuterJoin(kv2)
    full.foreach(println)
  }
}
