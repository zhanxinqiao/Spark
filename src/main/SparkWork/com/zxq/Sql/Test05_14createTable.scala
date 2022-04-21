package com.zxq.Sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Test05_14createTable {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .config(new SparkConf())
      .master("local")
      .appName("14tables")
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    import session.implicits._


    val value: Dataset[String] = session.read.textFile("/tem/SparkWork/14/user_artist_data.txt")
    val ds: DataFrame = value.map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(0), strings(1), strings(2).toLong)
    })(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaLong))
      .toDF("userid", "artistid", "playcount")


    ds.createTempView("music")
    println("music表为:")
    ds.show(10)

    //1）统计非重复的用户个数。
//    session.sql(
//      " select count(user) "+
//       " from ("+
//      " select distinct(userid) as user from music ) as a "
//    ).show()

    //2）统计用户听过的歌曲总数。
    session.sql("select count(playcount) from music ").show()

    //3）找出ID为”1000002”的用户最喜欢的10首歌曲(即播放次数最多的10首歌）
    session.sql("select artistid "+
    " from music "+
    " where userid='1000002' "+
    " order by playcount desc "+
    " limit 10 "
    ).show()



  }
}
