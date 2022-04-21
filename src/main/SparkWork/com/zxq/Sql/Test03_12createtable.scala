package com.zxq.Sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Test03_12createtable {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("people")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    import session.implicits._


    val df: DataFrame = List(
      ("001", "张三", "1", "北京", "112233"),
      ("002", "李四", "1", "武汉", "112233"),
      ("003", "王五", "1", "南京", "112233"),
      ("004", "罗六", "1", "东京", "112233"),
      ("005", "陈七", "0", "江苏", "112233"),
      ("006", "江九", "0", "南昌", "112233"),
      ("007", "刘十", "0", "越南", "112233"),
      ("008", "岳一", "0", "缅甸", "112233"),
      ("009", "顾二", "1", "上海", "112233"),
      ("010", "胡八", "0", "湖南", "112233")
    ).toDF("id", "name", "sex", "address", "telephone")

    df.createTempView("people")

    session.sql("select * from people").show()



  }
}
