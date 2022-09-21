package com.zxq.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test05_sql_onhive {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("test in hive")
      //这里需要使用的是hive服务器端的映射，即:node1上面配置文件中hive-site.xml里面的一个<property>，表示客户端寻找服务端
      .config("hive.metastore.uris", "thrift://node3:9083")
      //启用hive
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = ss.sparkContext
    sc.setLogLevel("ERROR")
    //启用隐式转换，这里一般是指创建DataFrame的toDF()函数
    import ss.implicits._

    //创建一个临时表，这个表被归结到了null库中，而且是isTemporary表，即临时表
    //此时在hive中是看不到它的存在的
    val df1: DataFrame = List(
      "zhangsan",
      "lisi"
    ).toDF("name")
    df1.createTempView("ooxx")

    ss.sql("select * from ooxx").show()
    //将DataFrame类型的数据以表的形式写入在hive内，成为一个表，此时是没有数据的，只有数据类型
    df1.write.saveAsTable("oxox")
    //以上方式创建的hive表，当向表中插入数据时，hive中不可见，但是可以在当前页面可查(类似临时表)
    ss.sql("insert into oxox select * from ooxx")

  //以DDLSQL创建的表是永久存在的，此时若是将临时表的数据插入，可以成功而且永久存在
    ss.sql("create table oxxo(name string)")
    ss.sql("insert into oxxo select * from ooxx")
    ss.sql("select * from oxox").show()
    ss.catalog.listTables().show()


    //使用DDLSQL创建的表是永久存在的，可以在hive中的default库中查看到，此时表是永久存在的
//    ss.sql("create table xxoo (id int)")  //DDL
//    ss.sql("insert into xxoo values (3),(6),(7)")  //DML

    ss.catalog.listTables().show()

    //从这里可以看出来，当我们使用default的库时，我们用DDL创建的表可以看到，
    // 同时我们用createTempView注册的临时表也可以看到，它的库名是空
    ss.sql("use default")
    val df: DataFrame = ss.sql("show tables")
    df.show()


    /*
    * 所以我们如果用DataFrame创建临时表时，要发现临时表不入hive的数据库
    * 若要将数据写入，则用DDLSQL创建一个表(和临时表数据想同)，再来复制这张表的数据即可，
    * 我们也可以使用DataFrame.write.saveAsTable(TableName)将表的结构写入hive中，表名为TableName
    * 若我们将临时表内容复制进去,此时hive内的表是没有数据的,但是代码内的表有数据
    * 此处是一个疑问??????????????????????????????????????
    * */
  }

}
