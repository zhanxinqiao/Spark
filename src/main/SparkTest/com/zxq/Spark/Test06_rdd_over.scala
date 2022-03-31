package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Test06_rdd_over {
  def main(args: Array[String]): Unit = {
    //wordcount * 10
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("topN")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

//    val data: RDD[String] = sc.parallelize(List(
//      "hello world",
//      "hello spark",
//      "hello world",
//      "hello hadoop",
//      "hello world",
//      "hello msb",
//      "hello world"
//    ))
//    val words: RDD[String] = data.flatMap(_.split(" "))
//    val kv: RDD[(String, Int)] = words.map((_, 1))
//    val res: RDD[(String, Int)] = kv.reduceByKey(_+_)
////    val res01: RDD[(String, Int)] = res.map(x => (x._1, x._2 * 10))
//     val res01: RDD[(String, Int)] = res.mapValues(x => x * 10)
//     val res02: RDD[(String, Iterable[Int])] = res01.groupByKey()
//     res02.foreach(println)

    implicit val sdfsdf=new Ordering[(Int,Int)]{
      override def compare(x: (Int, Int), y: (Int, Int))=y._2.compareTo(x._2)
    }

    val file: RDD[String] = sc.textFile("data/tqdata")
    val data: RDD[(Int, Int, Int, Int)] = file.map(line => line.split("\t")).map(arr => {
      //2019-6-1	39
      val arrs: Array[String] = arr(0).split("-")
      //(year,month,day,wd)
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    })

                               //分组 取TopN(排序)
    println("-------------①-------------------")
    //第一代
    //用了groupByKey容易OOM(Out Of Memory内存用完了)  且自己的算子实现了函数:去重、排序
    //将数据集转换为tuple2((year,month),(day,wd))
    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    //将相同年份分为一组 （日期，温度）为一个元素输出
    val res: RDD[((Int, Int), List[(Int, Int)])] = grouped.mapValues(arr => {
      //用HashMap数据结构存储(day,wd)
      val map = new mutable.HashMap[Int, Int]()
      //遍历数据集取到每天最大的值(即达到去重的目的)
      arr.foreach(x => {
        if (map.get(x._1).getOrElse(0) < x._2)
          map.put(x._1, x._2)
      })
      //将温度进行排序  这里 new Ordering 调用了隐式转换的的排序方法
      map.toList.sorted(new Ordering[(Int, Int)] {
        override def compare(x: (Int, Int), y: (Int, Int)) = y._2.compareTo(x._2)
      })
    })
    res.foreach(println)

    println("---------------②-----------------")
     //第二代
    //用了groupByKey  容易OOM(Out Of Memory内存用完了) 但是有取巧:spark rdd reduceByKey 的取max间接达到去重  让自己的算子变动简单点
    /*
    * 数据为((年，月，日)，温度)
    * reduced:逻辑实现了 将 年、月、日 分组为Key 获取当天最大值 达到去重的目的
    * maped:将数据((年、月、日),温度) 转换为 ((年，月),(日，温度))
    * grouped2:按照相同的Key分组 key:(年，月)  Value:(日期，温度)
    * */
    val reduced2: RDD[((Int, Int, Int), Int)] = data.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x, y) => if (y > x) y else x)
    val maped2: RDD[((Int, Int), (Int, Int))] = reduced2.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
    val grouped2: RDD[((Int, Int), Iterable[(Int, Int)])] = maped2.groupByKey()
    //将值强转为list数组，然后取出前两个输出
    grouped2.mapValues(arr=>arr.toList.sorted.take(2)).foreach(println)

    println("---------------③-----------------")
    //第三代
    //用了groupByKey  容易OOM(Out Of Memory内存用完了) 取巧:用了Spark的RDD的reduceByKey去了重，用了sortByKey排序
    /*
    * sorted3:根据(年、月、日)为一组进行排序(降序)
    * reduced3: 将数据打散重新作为元组((年，月，日)，温度)  然后提取出相同Key里面的最高温度(去重操作)
    * grouped3:再次分组,将((年,月),(日，温度))作为元组组合  达到相同月份的每一天都是最高温度  且已经去重
    * */
    val sorted3: RDD[(Int, Int, Int, Int)] = data.sortBy(t4 => (t4._1, t4._2, t4._4), false)
    val reduced3: RDD[((Int, Int, Int), Int)] = sorted3.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x, y) => if (y > x) y else x)
    val grouped3: RDD[((Int, Int), (Int, Int))] = reduced3.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
//    grouped3.reduceByKey(_)
    grouped3.foreach(println)

    println("---------------④-----------------")
    //第四代
    //用了groupByKey  容易OOM 取巧:用了Spark的RDD sortByKey排序，没有破坏多级shuffle的Key的子集关系
    val sorted4: RDD[(Int, Int, Int, Int)] = data.sortBy(t4 => (t4._1, t4._2, t4._4) , false)
    val grouped4: RDD[((Int, Int), Iterable[(Int, Int)])] = sorted4.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    grouped4.foreach(println)

    println("---------------⑤-----------------")
          //第五代
    //分布式计算的核心:调优天下无敌：combineByKey
    //分布式是并行的，离线批量计算有个特征：就是后续步骤(stage)依赖其一步骤(stage)
    //如果前一步骤(stage)能够加上正确的combineByKey
    //我们自定义的combineByKey的函数  是尽量压缩内存中的数据
    val kv: RDD[((Int, Int), (Int, Int))] = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4)))
    val res6: RDD[((Int, Int), Array[(Int, Int)])] = kv.combineByKey(
      //准备放三条数据 (日期，温度)，(日期，温度)，(日期，温度)
      //第一条记录怎么放：
      (v1: (Int, Int)) => {
        Array(v1, (0, 0), (0, 0))
      },
      //第二条，以后后续的怎么放:
      (oldv: Array[(Int, Int)], newv: (Int, Int)) => {
        //去重 排序
        var flag = 0 //0,1,2新进来的元素特征  : 日 a) 相同 1) 温度大 2) 温度小 日 b) 不同
        for (i <- 0 until oldv.length) {
          //日期相同则放进区域1，区域2 进行最大值的选择
          if (oldv(i)._1 == newv._1) {
            if (oldv(i)._2 < newv._2) {
              flag = 1
              oldv(i) = newv
            } else {
              flag = 2
            }
          }
        }
        //日期值不同  则将不同日期的值放在第三个位置 即我们设定的缓冲区  达到间接去重的目的
        if (flag == 0) {
          oldv(oldv.length - 1) = newv
        }
        //对三个数据进行排序(进行上面隐式转换的排序)

//   方法一：会产生GC     oldv.sorted
        //方法二  快排  懒加载函数
        scala.util.Sorting.quickSort(oldv)
        oldv
      },
      (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
        //关注去重
        val union: Array[(Int, Int)] = v1.union(v2)
        union.sorted
      }
    )
    //最后得到的是一个数组  我们要将其转换为List列表
  res6.map(x=>(x._1,x._2.toList))foreach(println)

  }

}
