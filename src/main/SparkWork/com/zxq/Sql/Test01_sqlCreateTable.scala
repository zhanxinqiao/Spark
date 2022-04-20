package com.zxq.Sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import scala.beans.BeanProperty


class User extends Serializable{
  @BeanProperty
  var id:String=""
  @BeanProperty
  var name:String=""
  @BeanProperty
  var age:Int=0
}


object Test01_sqlCreateTable {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("CreateTable")
      .master("local")
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

               //定义工具类然后创建表有两个经典方式
    // 方式一
    val data: RDD[String] = sc.textFile("/tem/spark/user.txt")
    val user: User = new User
    val rddBean: RDD[User] = data.map(_.split(",")).map {
      arr => {
        user.setId(arr(0))
        user.setName(arr(1))
        user.setAge(arr(2).toInt)
        user
      }
    }

    val df1: DataFrame = session.createDataFrame(rddBean, classOf[User])
    df1.show()
    df1.printSchema()

    println("===================================================")

    //方式二
    val ds: Dataset[String] = session.read.textFile("/tem/spark/user.txt")
    val ds1: Dataset[(String, String, Int)] = ds.map(line => {
      val strings: Array[String] = line.split(",")
      (strings(0), strings(1), strings(2).toInt)
    })(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaInt))

    val df2: DataFrame = ds1.toDF("id", "name", "age")
    df2.show()
    df2.printSchema()
  }
}
