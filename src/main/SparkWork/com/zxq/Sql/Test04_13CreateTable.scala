package com.zxq.Sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

object Test04_13CreateTable {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .config(new SparkConf())
      .master("local")
      .appName("13Table")
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    import session.implicits._

    //创建tbDate表
    val ds1: RDD[String] = sc.textFile("/tem/SparkWork/13/tbDate.txt")
    val value1: RDD[Row] = ds1.map(_.split(",")).map(strings => Row.apply(
      strings(0),
      strings(1).toInt,
      strings(2).toInt,
      strings(3).toInt,
      strings(4).toInt,
      strings(5).toInt,
      strings(6).toInt,
      strings(7).toInt,
      strings(8).toInt,
      strings(9).toInt
    ))

    val fields1: Array[StructField] = Array(
      StructField("Dateid", DataTypes.StringType, nullable = true),
      StructField("TheyearMonth", DataTypes.IntegerType, nullable = true),
      StructField("Theyear", DataTypes.IntegerType, nullable = true),
      StructField("Themonth", DataTypes.IntegerType, nullable = true),
      StructField("Thedate", DataTypes.IntegerType, nullable = true),
      StructField("Theweek", DataTypes.IntegerType, nullable = true),
      StructField("theweeks", DataTypes.IntegerType, nullable = true),
      StructField("thequot", DataTypes.IntegerType, nullable = true),
      StructField("thetenday", DataTypes.IntegerType, nullable = true),
      StructField("thehalfmonth ", DataTypes.IntegerType, nullable = true)
    )
    val df1: DataFrame = session.createDataFrame(value1, StructType.apply(fields1))
    df1.createTempView("toDate")
    println("toDate表:")
    df1.show(10)

    //创建tdStock表
    val ds2: Dataset[String] = session.read.textFile("/tem/SparkWork/13/tbStock.txt")
    val df2: DataFrame = ds2.map(line => {
      val strings: Array[String] = line.split(",")
      (strings(0), strings(1), strings(2))
    })(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING)).toDF("Ordernumber", "Locationid", "DateID")
    df2.createTempView("tdStock")
    println("tdStock表:")
    df2.show(10)

    //创建toStockDetail表
    val ds3: RDD[String] = sc.textFile("/tem/SparkWork/13/tbStockDetail.txt")
    val value3: RDD[Row] = ds3.map(_.split(",")).map(strings => Row.apply(
      strings(0), strings(1).toInt, strings(2), strings(3).toInt, strings(4).toDouble, strings(5).toDouble
    ))

    val fields3: Array[StructField] = Array(
      StructField("Ordernumber", DataTypes.StringType, nullable = true),
      StructField("Rownum", DataTypes.IntegerType, nullable = true),
      StructField("Itemid", DataTypes.StringType, nullable = true),
      StructField("Qty", DataTypes.IntegerType, nullable = true),
      StructField("Price", DataTypes.DoubleType, nullable = true),
      StructField("Amount", DataTypes.DoubleType, nullable = true)
    )
    val schema3: StructType = StructType.apply(fields3)
    val df3: DataFrame = session.createDataFrame(value3, schema3)
    df3.createTempView("tbStockDetail")
    println("tbStockDetail表:")
    df3.show(10)

//    1）	计算所有订单中每年的销售单数、销售总额。
//        session.sql("select Theyear,count(tdStock.Ordernumber),sum(Amount) "+
//          "from toDate,tbStockDetail,tdStock "+
//          " where  toDate.Dateid=tdStock.DateID "+
//          " and tdStock.Ordernumber=tbStockDetail.Ordernumber "+
//          " group by Theyear "
//        ).show()

    //2）	计算所有订单每年最大金额订单的销售额
//    session.sql(" select Theyear,first(Ordernumber),max(tt.sA) "+
//      " from ("+
//      " select Theyear,tbStockDetail.Ordernumber,sum(Amount) as sA "+
//      " from toDate,tbStockDetail,tdStock "+
//      " where toDate.Dateid=tdStock.DateID "+
//      " and tdStock.Ordernumber=tbStockDetail.Ordernumber "+
//      " group by Theyear,tbStockDetail.Ordernumber "+
//      " order by Theyear,sum(Amount) desc ) as tt"+
//      " group by Theyear "+
//      " order by Theyear "
//    ).show()

    //3）	计算所有订单每年最畅销货品。
    session.sql(" select Theyear,first(Ordernumber),max(tt.sA) "+
      " from ("+
      " select Theyear,tbStockDetail.Ordernumber,sum(Qty) as sA "+
      " from toDate,tbStockDetail,tdStock "+
      " where toDate.Dateid=tdStock.DateID "+
      " and tdStock.Ordernumber=tbStockDetail.Ordernumber "+
      " group by Theyear,tbStockDetail.Ordernumber "+
      " order by Theyear,sum(Qty) desc ) as tt"+
      " group by Theyear "+
      " order by Theyear "
    ).show()
  }
}
