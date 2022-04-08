package com.zxq.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Properties

object Test03_sql_jdbc {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder() //构建
      .master("local")
      .appName("zxq")
      .config("spark.sql.shuffle.partitions", "1")  //设置并行度  默认会有200并行度的参数  即shuffle
      .getOrCreate()
    val sc: SparkContext = ss.sparkContext
    sc.setLogLevel("ERROR")

    /*Properties类表示一组持久的属性。 Properties可以保存到流中或从流中加载。属性列表中的每个键及其对应的值都是一个字符串。 */
    val pro: Properties = new Properties()
    //设置一组键值对  URL ==>  jdbc 连接方式 连接的主机IP  连接的主机数据库名
      pro.put("url","jdbc:mysql://192.168.182.100/test")
    //设置一组键值对 设置用户  user ==>  用户名
     pro.put("user","root")
    //设置一组键值对  设置用户  password ==>  密码
     pro.put("password","123456")
    //设置一组键值对  设置连接驱动对象  driver ==> 驱动包
     pro.put("driver","com.mysql.jdbc.Driver")

    /*
    SparkSession.read:返回一个DataFrameReader ，可用于读取作为DataFrame的非流式数据。
    SparkSession.read.jdbc:构造一个表示数据库表的DataFrame ，该数据库表可通过名为 table 和连接属性的 JDBC URL url 访问*/
    val deptDF: DataFrame = ss.read.jdbc(pro.get("url").toString, "dept", pro)
    val empDF: DataFrame = ss.read.jdbc(pro.get("url").toString, "emp", pro)

    /*
    * 使用给定名称创建本地临时视图。此临时视图的生命周期与用于创建此数据集的SparkSession相关联。
     本地临时视图是会话范围的。它的生命周期是创建它的会话的生命周期，即当会话终止时它会被自动删除。
     它不依赖于任何数据库，即我们不能使用db1.view1来引用本地临时视图。*/
     deptDF.createTempView("depttab")
     empDF.createTempView("emptab")

    //使用 SparkSession 执行 SQL 查询，将结果作为DataFrame返回。用于 SQL 解析的方言可以使用“spark.sql.dialect”进行配置。(即其他SQL数据库语法)
    val resDF: DataFrame = ss.sql("select depttab.dname,emptab.hiredate from depttab join emptab on depttab.deptno=emptab.depton where emptab.id<100 ")
    resDF.show()
    resDF.printSchema()

    println("========================================================")
    println(resDF.rdd.partitions.length) //打印默认连接的分区数(并行度)  这里默认是200
    /*   resDF.coalesce 操作详解:
    * 当请求的分区较少时，返回一个新的数据集，该数据集恰好具有numPartitions个分区。如果请求的分区数更大，它将保持当前的分区数。
    * 与在RDD上定义的 coalesce 类似，此操作会导致窄依赖关系，
    * 例如，如果您从 1000 个分区转到 100 个分区，则不会进行 shuffle，而是 100 个新分区中的每一个都将占用当前分区中的 10 个。
    * 然而，如果你正在做一个剧烈的合并，例如 numPartitions = 1，这可能会导致你的计算发生在比你想要的更少的节点上（例如，
    * 在 numPartitions = 1 的情况下是一个节点）。为避免这种情况，您可以调用 repartition。这将添加一个 shuffle 步骤，
    * 但意味着当前的上游分区将并行执行（无论当前分区是什么）
    *
    *    修改数据分区数(并行度)的方法  :
    * ① :SparkSession..config("spark.sql.shuffle.partitions", 1)  即在构建SparkSession时配置其配置文件
    * ② :DataFrame.coalesce(1)  将连接数据库构建的DataFrame对象中添加一个参数 分区
    * */
    val resDF01: Dataset[Row] = resDF.coalesce(1)
    println(resDF01.rdd.partitions.length) //打印更改之后的分区数(并行度)  这里改为了 1
    println("========================================================")
    resDF.write.jdbc(pro.get("url").toString,"bbbb",pro)

    //什么数据源拿到的都是DS(DataSet)/DF(DataFrame)

    /* SparkSession.read:返回一个DataFrameReader , 可用于读取作为DataFrame的非流式
       SparkSession.read.jdbc: 构造一个表示数据库表的DataFrame, 该数据库表可通过名为table和连接属性的JDBC URL url访问 */
    val jdbcDF: DataFrame = ss.read.jdbc(pro.get("url").toString, "bbbb", pro)
    jdbcDF.show()

    /*  使用给定名称创建本地临时视图。此临时视图的生命周期与用于创建此数据集的SparkSession相关联。
    *   本地临时视图是会话范围的。它的生命周期是创建它的会话的生命周期，即当会话终止时它会被自动删除。
    *  它不依赖与任何数据库，即我们不能使用db1.view1来引用本地临时视图
    *     */
    jdbcDF.createTempView("ooxx")

    //使用SparkSession 执行SQL查询, 将结果作为DataFrame返回。  用于SQL解析的方言可以使用“spark.sql.dialect“ 进行配置”
    ss.sql("select * from ooxx").show()
    /*   DataFrame.write  用于将非流数据集的内容保存到外部存储的接口。
    *    DataFrame.write.jdbc:通过JDBC将DataFrame的内容保存到外部数据库表中。如果表已经存在于外部数据库中，此函数行为取决于模式函数指定的保存模式(默认抛出异常)
    *  不要再大型集群上面并行创建太多分区；否则Spark可能会使你的外部数据库崩溃。
    * */
    jdbcDF.write.jdbc(pro.get("url").toString,"xxxx",pro)
  }
}
