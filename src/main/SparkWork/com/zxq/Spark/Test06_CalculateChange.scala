package com.zxq.Spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

object Test06_CalculateChange {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CalculateChange")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val fm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

//    val data: RDD[(String, Double)] = sc.textFile("data/000061.csv").filter(x => !x.contains("date")).map(y => {
//      val data_split: Array[String] = y.split(",")
//      val time: Date = fm.parse(data_split(0))
//      //(时间,(时间,收盘价格))
//      (time.getTime, (data_split(0), data_split(3).toDouble))
//    }).sortByKey().map(x => (x._2._1, x._2._2))
//
//    val queue: mutable.Queue[Double] = new mutable.Queue[Double]()

//单独预测涨幅时
//    val zhangfu: RDD[(Any, Double)] = data.map(x => {
//      queue.enqueue(x._2) //进
//      if (queue.size == 2) {
//        val cha: Double = (queue.last - queue.head) / queue.head //(当日收盘价 - 昨日收盘价)/昨日收盘价
//        queue.dequeue() //出
//        (x._1, cha)
//      } else {
//        (0, 0)
//      }
//    })
//    zhangfu.foreach(println)

    //切割数据返回(时间格式的时间，还有收盘时间)
        val data: RDD[ (String, Double)] = sc.textFile("data/000061.csv").filter(x => !x.contains("date")).map(y => {
//    val data: RDD[ (String, Double)] = sc.textFile("/tem/spark/000061.csv").filter(x => !x.contains("date")).map(y => {
      val data_split: Array[String] = y.split(",")
      val time: Date = fm.parse(data_split(0))
      //(时间,收盘价格)
      (time.getTime, (data_split(0), data_split(3).toDouble))
    }).sortByKey().map(x=>(x._2._1,x._2._2))//按时间排序后 返回(时间,收盘价格)
    data.foreach(println)

    val queue: mutable.Queue[Double] = new mutable.Queue[Double]()

    //返回(时间格式的时间，还有收盘时间)
    val MovingAvg: RDD[(Any, Double)] = data.map(x => {
      queue.enqueue(x._2) //进
      if (queue.size == 2) {
        val cha: Double = (queue.last - queue.head) / queue.head //(当日收盘价 - 昨日收盘价)/昨日收盘价
        queue.dequeue() //出
        (x._1,cha)
      } else {
        (0,0)
      }
    })
    val MovingAvg2: RDD[(Any, Double)] = MovingAvg.filter(x => x != 0)
    MovingAvg2.foreach(println)

    queue.clear()
    val MovingAvgValue: RDD[(Any, Double, Double)] = MovingAvg2.map(x => {
      queue.enqueue(x._2) //进
      if (queue.size >= 10) {
        val avg: Double = queue.sum / 10
        queue.dequeue() //出
        (x._1, avg, x._2)
      } else {
        (0, 0, 0)
      }
    })

    val MovingAvgValue2: RDD[(Any, (Double, Double))] = MovingAvgValue.filter(x => x._1 != 0).map(x => (x._1, (x._2, x._3)))
    MovingAvgValue2.partitionBy(new DataPartition)
    MovingAvgValue2.foreach(println)
  }
}
class DataPartition extends Partitioner {
  override def numPartitions: Int = 4

  override def getPartition(key: Any): Int = {
    val data: String = key.toString.substring(0, 4).trim()
    if(data=="2013"){0}
    else if (data=="2014"){1}
    else if (data=="2015"){2}
    else {3}
  }

  override def equals(obj: Any): Boolean = obj match {
    case dataPartition: DataPartition =>dataPartition.numPartitions==numPartitions
    case _=>false
  }
}
