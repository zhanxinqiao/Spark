package com.zxq.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession}

import java.sql.Struct
import scala.beans.BeanProperty

class Person extends Serializable {
  @BeanProperty
  var name: String = ""
  @BeanProperty
  var age: Int = 0
  @BeanProperty
  var sex: Int = 0
}

object Test02_sql_api01 {
  def main(args: Array[String]): Unit = {
    //sql 字符串 -> dataset 对RDD的一个包装(优化器) -> 只有RDD才能触发DAGScheduler

    //配置文件变化
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test") //①
    val session: SparkSession = SparkSession
      .builder() //创建一个SparkSession.Builder来构造一个SparkSession 。
      .config(conf) //这边也可以直接缩减代码为 .config(new SparkConf())
      //      .appName("test") 如果①处只有new SparkConf() 后没有加setAppName则可以添加
      //      .master("local") 如果①处只有new SparkConf() 后没有加setMaster则可以添加
      //      .enableHiveSupport() 若开启这个选项 spark SQL on hive 才支持DDL,没开启,spark只有catalog
      .getOrCreate() /*获取现有的SparkSession ，或者，如果没有现有的，则根据此构建器中设置的选项创建一个新的。
     此方法首先检查是否存在有效的线程本地 SparkSession，如果有，则返回那个。然后它检查是否有一个有效的全局默认 SparkSession，如果有，返回那个。如果不存在有效的全局默认 SparkSession，则该方法创建一个新的 SparkSession 并将新创建的 SparkSession 分配为全局默认值。
     如果返回现有的 SparkSession，此构建器中指定的配置选项将应用于现有的 SparkSession。*/
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._ //调用session
    /** 将此强类型数据集合转换为通用数据框。与 Dataset 操作处理的强类型对象相比，
     * Dataframe 返回通用Row对象，这些对象允许按序号或名称访问字段。
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
      //toDF:将此强类型数据集合转换为具有重命名列的通用DataFrame 。这在从元组的 RDD 转换为具有有意义名称的DataFrame时非常方便。
    ).toDF("line")

    dataDF.createTempView("ooxx2")

    /** 发现只有ooxx2这个表  之前创建的ooxx表不见了
     * 代表createTempView创建的是在临时库中创建临时表 只在当前代码段中有效
     */
    session.catalog.listTables().show()

    val df: DataFrame = session.sql("select * from ooxx2") //查询ooxx2表
    df.show() //输出打印
    df.printSchema() //以树的结构将列的相应参数(架构)打印出来
    println("===================================")
    //写SQL查询   方法①
    session.sql("select word,count(*)  from (select explode(split(line,' ')) as word from ooxx2) as tt group by tt.word").show()

    println("===================================")
    //写SQL查询   方法②
    // 面向api的时候  df相当于 from table
    //selectExpr:  选择一组 SQL 表达式,这是接受 SQL 表达式的select的变体。 ==>  select 查询
    val res: DataFrame = dataDF.selectExpr("explode(split(line,' ')) as word").groupBy("word").count()
    res.show() //输出打印
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
    frame.show() //输出打印
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
    val rdd: RDD[String] = sc.textFile("data/person.txt")
    /* 第一种方式: row类型的rdd  + structType    */
    //  //数据+元数据 == df  就是一张表！！！

    //
    //  /**  1.数据: RDD[Row] */
    //     //Row.apply:此方法可用于构造具有给定值的Row  Row常见类型有(Seq Array) 因为调用的是toSeq、toArray等方法
    //      val tddRow: RDD[Row] = rdd.map(_.split(" ")).map(arr=>Row.apply(arr(0),arr(1).toInt))
    //
    //      /*   2. 元数据 : StructType */
    //      val fields: Array[StructField] = Array(
    //   /**      StructType 中的字段。
    //           参形：
    //           name – 此字段的名称。
    //           dataType – 此字段的数据类型。
    //           nullable – 指示此字段的值是否可以为空值。
    //           metadata – 此字段的元数据。如果列的内容没有被修改，例如在选择中，元数据应该在转换过程中被保留  **/
    //        //StructField.apply() ==> StructField()
    //        StructField("name", DataTypes.StringType, nullable = true),
    //        StructField.apply("age", DataTypes.IntegerType, nullable = true)
    //      )
    //
    //      val schema: StructType = StructType.apply(fields)
    //      //使用给定模式从包含Row的RDD创建一个DataFrame 。确保提供的 RDD 的每一Row的结构与提供的模式匹配是很重要的。否则会出现运行时异常。
    //      val dataFrame: DataFrame = session.createDataFrame(tddRow, schema)
    //      dataFrame.show()
    //      dataFrame.printSchema()
    //      //以下DQL语法可被执行
    //      dataFrame.createTempView("person")
    //      session.sql("select * from person").show()
    //
    //     //以下DDL语法不可被执行
    //     //原因如下: 开启了Hive即可用 metastore(元数据管理)    未开启则用的是catalog(目录)
    //     //metastore:可用于所有的SQL语句       catalog:只可用于简单的DQL
    ////     session.sql("create table ooxx3(name string , age int)")
    ////     session.catalog.listTables().show()


    println("=============================================================")
    /*   第一点一版本: 动态封装   */
    val userSchema: Array[String] = Array(
      "name string",
      "age int",
      "sex int"
    )


    /*   1. ROW RDD   */
    def toDataType(vv: (String, Int)) = {
      /**
       * vv._2 ==> (zhangsan,0)里面的数字
       * userSchema(vv._2) ==> 取出userSchema数组里面对应下标 的字符串 ("name string")
       * *这里就是已知数据  然后定义一个 数据类型数组  将数据规整化之后  取出相应类型 最后返回相应类型**
       *
       */
      userSchema(vv._2).split(" ")(1) match {
        case "string" => vv._1
        case "int" => vv._1.toInt
      }
    }

    val rowRdd: RDD[Row] = rdd.map(_.split(" ")).map(x => x.zipWithIndex)
      //x == [(zhangsan,0),(18,1),(0,2)]   即 zipWithIndex 是将一个有序集合的各元素 创建一个循环计数器在其元素后 计数元素从0开始
      .map(x => x.map(toDataType(_)))
      // x == [String,Int,Int]
      .map(x => Row.fromSeq(x))

    /*  2. structType          */
    def getDataType(v: String) = {
      v match {
        case "string" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
      }
    }

    /*
    * StructType 中的字段。
            参形：
            name – 此字段的名称。
            dataType – 此字段的数据类型。
            nullable – 指示此字段的值是否可以为空值。
            metadata – 此字段的元数据。如果列的内容没有被修改，例如在选择中，元数据应该在转换过程中被保留*/
    val fields1: Array[StructField] = userSchema
      .map(_.split(" "))
      .map(x => StructField.apply(x(0), getDataType(x(1)))) //(name string)

                 /*  以下方式① ② 都可以将数据类型结构化 (即表的类型等元数据)*/
    //①  对于StructType对象，可以通过名称提取一个或多个StructField 。如果提取多个StructField ，
    // 将返回一个StructType对象。如果提供的名称没有匹配的字段，它将被忽略。
    // 对于提取单个StructField的情况，将返回null
    val schema: StructType = StructType.apply(fields1)
    //② 为给定的 DDL 格式字符串创建 StructType，该字符串是字段定义的逗号分隔列表，例如，a INT、b STRING
    val schema01: StructType = StructType.fromDDL("name string,age int,sex int")
    //创建DataFrame的对象 即表
    val df1: DataFrame = session.createDataFrame(rowRdd, schema)
    df1.show()
    df1.printSchema()

    println("=============================================================")
    /*       第二种方式: bean类型的rdd + javabean    */
    // 数据 + 元数据 ==df 就是一张表！！！
    val p: Person = new Person //①
    //1, mr,spark  => pipline(管道) => iter(迭代器) 一次内存飞过一条数据 :: -> 这一条记录完成读取/计算/序列化
    //2, 分布式计算, 计算逻辑由 Driver 序列化 , 发送给其他JVM的Executor中执行
    val rddBean: RDD[Person] = rdd.map(_.split(" "))
      .map(arr => {
//        val p = new Person     //②
        /*    将构造类放在②号位置时  构造类不需要(extends Serializable)
        若将构造类放在①号位置时  则需要(extends Serializable)
                  */
        p.setName(arr(0))
        p.setAge(arr(1).toInt)
        p.setSex(arr(2).toInt)
        p
      })
    val df2: DataFrame = session.createDataFrame(rddBean, classOf[Person])
    df2.show()
    df2.printSchema()

    println("=============================================================")

    val ds01: Dataset[String] = session.read.textFile("data/person.txt")
    val person: Dataset[(String, Int)] = ds01.map(line => {
      val strs: Array[String] = line.split(" ")
      (strs(0), strs(1).toInt)
      //Encoders :scala中的一种编码器  可对多种格式进行编码(eg:tuple,STRING,scalaInt等
      // 相当于自定义类继承了Serializable)
      //最终结果:在此处将一个RDD序列化为相应格式
    })(Encoders.tuple(Encoders.STRING, Encoders.scalaInt))
    //toDF:将此强类型数据集合转换为具有重命名列的通用DataFrame ,
    //这在从元组的 RDD 转换为具有有意义名称的DataFrame时非常方便。
    val cperson: DataFrame = person.toDF("name", "age")
    cperson.show()
    cperson.printSchema()

    /*大数据整体操作流程:
     *纯文本文件，不带自描述，string  不被待见的
     * 必须转结构化  再参与计算
     * 转换的过程可以由spark完成
     *存储到hive数仓
     *接数：源数据  不删除 不破坏
     * ETL  中间态
     *所有的计算发生在中间态
     * 中间态 ==>  一切以后续计算成本为考量
     * 选择文件格式类型
     * 分区 / 分桶存放   避免发生数据倾斜
    */

  }
}
