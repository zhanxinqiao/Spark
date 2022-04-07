package com.zxq.sql

import org.apache.spark.sql.catalog.{Database, Table}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, catalog}

object Test01_sql_basic {
  def main(args: Array[String]): Unit = {
     //sql 字符串 -> dataset 对RDD的一个包装(优化器) -> 只有RDD才能触发DAGScheduler

     //配置文件变化
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test") //①
    val session: SparkSession = SparkSession
      .builder()
      .config(conf) //这边也可以直接缩减代码为 .config(new SparkConf())
      //      .appName("test") 如果①处new SparkConf() 后没有加setAppName则可以添加
      //      .master("local") 如果①处new SparkConf() 后没有加setMaster则可以添加
      //      .enableHiveSupport() 若开启这个选项 spark SQL on hive 才支持DDL,没开启,spark只有catalog
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    //以session 为主的操作演示
    //  DataFrame DataSet[Row]
    // SQL为中心
    // catalog指的是注册表 即数据库的元数据(临时的库)
    val databases: Dataset[Database] = session.catalog.listDatabases() //列举出所有的数据库
    databases.show() //输出打印
    val tables: Dataset[Table] = session.catalog.listTables() //列举出默认库下所有的表
    tables.show()  //输出打印
    //返回在当前数据库中注册的所有函数列表 这包含所有临时功能及常见函数
    val functions: Dataset[catalog.Function] = session.catalog.listFunctions()
//    numRows - 要显示的行数
//    truncate -- 是否截断长字符串。如果为真，超过 20 个字符的字符串将被截断，所有单元格将右对齐
    functions.show(999,true)

    println("===================================")

    //读取非流式数据(json等格式) 然后返回DataFrame的类型  即加工读取的数据
    val df: DataFrame = session.read.json("data/json")
    df.show()  //输出打印
    df.printSchema()  //以树的结构将列的相应参数(架构)打印出来

    df.createTempView("ooxx")  //这个过程是 df通过session向catalog中注册表名

    session.catalog.listTables().show()  //注册表名之后 此时的默认数据库数据表中多了ooxx这张表

  //查询方法①  非交互型
    //这句话就是将一个SQL语句转换为DataFrame的对象
    // (表示最终执行的代码要是DataFrame对象)
    val frame: DataFrame = session.sql("select * from ooxx")
    frame.show()

 //查询方法②  交互型

    //导入scala的一个io库
    //追源码最后发现走的是new BufferedReader(new InputStreamReader(java.lang.System.in)))
    import scala.io.StdIn._
    while (true){
      val sql: String = readLine("input your sql : ")
      session.sql(sql).show()
    }
  }
}
