package com.zxq.Sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object Test02_easy_sqlCreateTable {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .config(new SparkConf())
      .master("local")
      .appName("CreateTable")
      .getOrCreate()

    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._
    val value: RDD[String] = sc.textFile("/tem/spark/user.txt")

                     /*简单方式也分为两种:*/
    //第一种：
    val rddRow: RDD[Row] = value.map(_.split(",")).map(arr => Row.apply(arr(0), arr(1), arr(1).toInt))
    val fields: Array[StructField] = Array(
      StructField("id", DataTypes.StringType, nullable = true),
      StructField("name", DataTypes.StringType, nullable = true),
      StructField.apply("age", DataTypes.IntegerType, nullable = true)
    )
    val schema: StructType = StructType.apply(fields)
    val df: DataFrame = session.createDataFrame(rddRow, schema)
    df.show()
    df.printSchema()

   println("==============================================")
    //第二种
    val userSchema: Array[String] = Array(
      "id string",
      "name string",
      "age int"
    )

    def toDataType(vv :(String,Int))= {
      userSchema(vv._2).split(" ")(1) match {
        case "string" => vv._1
        case "int" => vv._1.toInt
      }
    }

    val value1: RDD[Array[(String, Int)]] = value.map(_.split(",")).map(x => x.zipWithIndex)
    value1.foreach(println)
    val value2: RDD[Array[Any]] = value1.map(x => x.map(toDataType(_)))
    value2.foreach(println)
    val value3: RDD[Row] = value2.map(x => Row.fromSeq(x))

    def getDataType(v:String)={
      v match {
        case "string"=>DataTypes.StringType
        case "int"=>DataTypes.IntegerType
      }
    }

    val fields1: Array[StructField] = userSchema
      .map(_.split(" "))
      .map(x => StructField.apply(x(0), getDataType(x(1))))

    val schema1: StructType = StructType.apply(fields1)
    val df2: DataFrame = session.createDataFrame(value3, schema1)
    df2.show()
    df2.printSchema()
    }

}
