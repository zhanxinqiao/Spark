package com.zxq.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Test04_sql_standalone_hive {

  def main(args: Array[String]): Unit = {
    //如果没有活动的 SparkSession，则使用默认的 SQL conf。否则，使用会话的。
    val ss: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("hive1")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.warehouse.dir", "F:/Spark/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = ss.sparkContext
    sc.setLogLevel("ERROR")
    import ss.sql

           /*  这里面默认用的是default这个数据库   但是创建的表相关信息在("spark.sql.warehouse.dir", "F:/Spark/warehouse")文件夹中可以找到*/
//              ss.sql("create table xxx2(name string,age int)")
//              ss.sql("insert into xxx2 values ('zhangsan',18),('lisi',22)")

            /*这里创建了一个数据库  若创建数据表时没有指明数据库 则默认创建在default数据库中
                可在("spark.sql.warehouse.dir", "F:/Spark/warehouse") 中找到*/
//    sql("create database zxq")
//    sql("create table zxq.xxx (name string,age int)")
//    sql("create table zxq (name string,age int)")
//    sql("insert into zxq.xxx values ('zhangsan',18),('lisi',22)")
//    sql("insert into zxq values('zhangsan',18),('lisi',22)")


    /*    这个DQL语句会在("spark.sql.warehouse.dir", "F:/Spark/warehouse")文件夹中去查找xxx这个表
              若没有的话则报错     */
     //    ss.sql("select * from xxx").show()

    /*  这里查找所有表用的是default数据库  */
//    ss.catalog.listTables().show()  //①
    /*    这里查找表在 ("spark.sql.warehouse.dir", "F:/Spark/warehouse")文件夹中去查找
                 若找不到文件夹 则报错  或许①处可查到这个表  也不能使用
                  若要使用 则注释掉 SparkSession.config("spark.sql.warehouse.dir", "F:/Spark/warehouse") 行即可使用
                  切记  注释掉SparkSession.config("spark.sql.warehouse.dir", "F:/Spark/warehouse")时还要删除derby.log这个日志文件夹
    */
    ss.sql("select * from xxx2").show()
    println("========================")
    /*   切换数据库  此时是在zxq这个数据库文件夹下面 去查找xxx表文件夹  */
    sql("use zxq")
    ss.catalog.listTables().show()
    ss.sql("select * from xxx").show()

    /*************************************************************************
    # ***             这边是数据库相应的操作(一定要有数据库的概念)                 ***
    # ***   use default  时刻记得切换default数据库                             ***
    # ***   mysql只是一个软件 它可以创建很多库 database是隔离的                   ***
    # ***   spark / hive  是一样的                                           ***
    # *************************************************************************/





  }
}
