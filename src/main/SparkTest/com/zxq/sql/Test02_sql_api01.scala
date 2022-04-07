package com.zxq.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.Struct
import scala.beans.BeanProperty

class Person extends Serializable{
  @BeanProperty
  var name:String=""
  @BeanProperty
  var age :Int=0
}

object Test02_sql_api01 {
  def main(args: Array[String]): Unit = {
    //sql 字符串 -> dataset 对RDD的一个包装(优化器) -> 只有RDD才能触发DAGScheduler

    //配置文件变化
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test") //①
    val session: SparkSession = SparkSession
      .builder()
      .config(conf) //这边也可以直接缩减代码为 .config(new SparkConf())
      //      .appName("test") 如果①处只有new SparkConf() 后没有加setAppName则可以添加
      //      .master("local") 如果①处只有new SparkConf() 后没有加setMaster则可以添加
      //      .enableHiveSupport() 若开启这个选项 spark SQL on hive 才支持DDL,没开启,spark只有catalog
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._  //调用session
    /**将此强类型数据集合转换为通用数据框。与 Dataset 操作处理的强类型对象相比，
    *Dataframe 返回通用Row对象，这些对象允许按序号或名称访问字段。
     * Dataset[Row] 万能匹配对象
    */
    val dataDF: DataFrame = List(
      "hello world",
      "hello spark",
      "hello world",
      "hello zxq",
      "hello world",
      "hello spark",
      "hello world",
      "hello world"
    ).toDF("line")

    dataDF.createTempView("ooxx2")
    /**发现只有ooxx2这个表  之前创建的ooxx表不见了
     *代表createTempView创建的是在临时库中创建临时表 只在当前代码段中有效
     */
    session.catalog.listTables().show()

    val df: DataFrame = session.sql("select * from ooxx2")  //查询ooxx2表
    df.show()  //输出打印
    df.printSchema() //以树的结构将列的相应参数(架构)打印出来
    println("===================================")
     //方法①
    session.sql("select word,count(*)  from (select explode(split(line,' ')) as word from ooxx2) as tt group by tt.word").show()

    println("===================================")
    //方法②
    // 面向api的时候  df相当于 from table
    //selectExpr:  选择一组 SQL 表达式,这是接受 SQL 表达式的select的变体。 ==>  select 查询
    val res: DataFrame = dataDF.selectExpr("explode(split(line,' ')) as word").groupBy("word").count()
    res.show()  //输出打印
    res.printSchema() //以树的结构将列的相应参数(架构)打印出来

    /*
    * 以上两种方式明显第二种更快
    * */

    println("=====================================")
    /*
    * SaveMode:用于指定将 DataFrame 保存到数据源的预期行为。(Append:追加)
    * parquet:将DataFrame的内容以 Parquet 格式保存在指定路径。这相当于： format("parquet").save(path)
    * */
    res.write.mode(SaveMode.Append).parquet("SqlData/ooxx2")
    println("=====================================")
    /*
    * 返回一个DataFrameReader,可用于DataFrame的非流式数据。
    * sparkSession.read.parquet("path/file.parquet")
    * sparkSession.read.schema(schema).json("path/file.json")
    * */
    val frame: DataFrame = session.read.parquet("SqlData/ooxx2")
    frame.show()  //输出打印
    frame.printSchema() //以树的结构将列的相应参数(架构)打印出来

    /*
    基于文件的格式读取:
       session.read.parquet()
       session.read.textFile()
       session.read.json()
       session.read.csv()
    读取任何格式的数据源都要转换为DF:
        res.write.parquet()
        res.write.orc()
        res.write.text()
     */

//    session.read.textFile("dfsdf")

  //数据+元数据 == df  就是一张表！！！
  val rdd: RDD[String] = sc.textFile("data/person.txt")

  /**  1.数据: RDD[Row] */
     //Row.apply:此方法可用于构造具有给定值的Row  Row常见类型有(Seq Array) 因为调用的是toSeq、toArray等方法
      val tddRow: RDD[Row] = rdd.map(_.split(" ")).map(arr=>Row.apply(arr(0),arr(1).toInt))

      /*   2. 元数据 : StructType */
      val fields: Array[StructField] = Array(
   /**      StructType 中的字段。
           参形：
           name – 此字段的名称。
           dataType – 此字段的数据类型。
           nullable – 指示此字段的值是否可以为空值。
           metadata – 此字段的元数据。如果列的内容没有被修改，例如在选择中，元数据应该在转换过程中被保留  **/
        //StructField.apply() ==> StructField()
        StructField("name", DataTypes.StringType, nullable = true),
        StructField.apply("age", DataTypes.IntegerType, nullable = true)
      )

      val schema: StructType = StructType.apply(fields)
      val dataFrame: DataFrame = session.createDataFrame(tddRow, schema)
      dataFrame.show()
      dataFrame.printSchema()
      //以下DQL语法可被执行
      dataFrame.createTempView("person")
      session.sql("select * from person").show()

     //以下DDL语法不可被执行
     //原因如下: 开启了Hive即可用 metastore(元数据管理)    未开启则用的是catalog(目录)
     //metastore:可用于所有的SQL语句       catalog:只可用于简单的DQL
//     session.sql("create table ooxx3(name string , age int)")
//     session.catalog.listTables().show()

      //第一种方式: row类型的RDD+structType






  }
}
