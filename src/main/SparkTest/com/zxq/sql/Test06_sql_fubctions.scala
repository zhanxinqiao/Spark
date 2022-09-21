package com.zxq.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test06_sql_fubctions {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder()
      .appName("functions")
      .master("local")
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    import ss.implicits._ //隐式转换，被toDF调用
    val dataDF: DataFrame = List(
      ("A", 1, 90),
      ("B", 1, 90),
      ("A", 2, 50),
      ("C", 1, 80),
      ("B", 2, 60)
    ).toDF("name", "class", "score")

    dataDF.createTempView("users")

    /** 分组、排序统计 */

    //eg1:按名字降序，同时按名字分组，统计一个人的成绩总分
    ss.sql(" select name, " +
      " sum(score) " +
      " from users " +
      " group by name " +
      " order by name desc "
    ).show()

    //eg2: 查询一个所有的人名和成绩，人名降序，成绩升序(名字相同的情况下)
    ss.sql("select * from users order by name desc , score asc").show()

    /*udf(自定义函数)*/
    //eg1:简单自定义函数:将成绩乘以10，变为十倍
    ss.udf.register("ooxx", (x: Int) => {
      x * 10
    })
    ss.sql("select * ,ooxx(score) as ox from users").show()

    //eg2:复杂自定义函数
    // MyAggFun 用户定义的聚合函数  需要继承UDAF(UserDefinedAggregateFunction),并按步骤实现方法
    ss.udf.register("oxox", new MyAggFun)
    ss.sql("select name, " +
      " oxox(score) " +
      " from users " +
      " group by name "
    ).show()

    /*  case...  when ... */
    //eg1:将学生按成绩分为不同的等级
    ss.sql(" select *, " +
      " case " +
      " when score<= 100 and score>=90 then 'good'  " +
      " when score<90 and score >=80 then 'you' " +
      " else 'cha' " +
      " end as ox " +
      " from users "
    ).show()

    //eg2:将学生按不同的成绩分为不同的等级，并且统计人数
    ss.sql(" select " +
      " case " +
      " when score <=100 and score >=90 then 'good' " +
      " when score <90 and score >= 80 then 'you' " +
      " else 'cha' " +
      " end as ox ," +
      " count(*) " +
      " from users " +
      " group by " +
      " case " +
      " when score <=100 and score >=90 then 'good' " +
      " when score <90 and score >= 80 then 'you' " +
      " else 'cha' " +
      " end "
    ).show()


    /*  行列转换 */
    //eg:将学生名字分别和班级、成绩拼接起来  concat():拼接函数，需给出拼接的列名和拼接字符
    // split():切割函数，表示对某一数据集进行切割，此处是数据的两列
    // explode():爆炸函数，其中explode(array)使得结果中将array列表里的每个元素生成一行
    ss.sql(" select name , " +
      " explode( " +
      " split( " +
      " concat(class,' ',score) , ' ' " +
      " )) " +
      " as ox " +
      " from users"
    ).show()

    /* 开窗函数 */
    /*  依赖 fun()  over (partition by ... order by ...)  partition by:分组  order by :排序
     开窗函数:
             1.能够作为开窗函数的聚合函数（sum，avg，count，max，min）
             2.rank，dense_rank。row_number等专用开窗函数。
     （1） row_number() over()：对相等的值不进行区分，相等的值对应的排名不相同，序号从1到n连续。
     （2） rank() over()：相等的值排名相同，但若有相等的值，则序号从1到n不连续。如果有两个人都排在第3名，则没有第4名。
     （3） dense_rank() over()：对相等的值排名相同，但序号从1到n连续。如果有两个人都排在第一名，则排在第2名（假设仅有1个第二名）的人是第3个人。
     （4） ntile( n ) over()：可以看作是把有序的数据集合平均分配到指定的数量n的桶中,将桶号分配给每一行，排序对应的数字为桶号。如果不能平均分配，
          则较小桶号的桶分配额外的行，并且各个桶中能放的数据条数最多相差1。 */


    //eg1: 按成绩排名  这里用到 rank() 和 row_number() 两个函数
    // rank():同组内相同的成绩是并列的排名   row_number():同组内相同的成绩，按照名字的顺序进行排序(升序)，非并列
    ss.sql(" select * , " +
      " rank() over(partition by class order by score desc ) as rank , " +
      " row_number() over(partition by class order by score desc ) as number " +
      " from users "
    ).show()

    // eg2.1:统计一个班成绩的总数量，这里可以想成是统计这个班的考试次数
    //在这个办法中丢失了成绩和人名的信息 并不能够充分的展示信息
    ss.sql(" select class ,count(score) as num from users group by class ").show()

    // eg2.2:统计一个班成绩的总数量，这里可以想成是统计这个班的考试次数,并列出详细信息
    ss.sql(" select * ," +
      " count(score) over(partition by class ) as num " + //根据班级分组并统计总数  并不丢失数据
      " from users ").show()
  }

}

class MyAggFun extends UserDefinedAggregateFunction {
  //数据进来时结构化：调用函数时的传参顺序，逐一初始化
  override def inputSchema: StructType = {
    //之所以是数组类型，是因为传参可能是多位参数
    StructType(Array(StructField.apply("score", IntegerType, false))) //(列名，数据类型，是否为空)
  }

  //中间运算层数据的结构化  buffer层
  override def bufferSchema: StructType = {
    //求avg:  sum/count
    //结构化数据
    StructType.apply(Array(
      //求均值会产生两个数据   sum和count  这里便初始化两个值
      StructField.apply("sum", IntegerType, false), //(列名，数据类型，是否为空)
      StructField.apply("count", IntegerType, false) //(列名，数据类型，是否为空)
    ))

  }

  //返回值数据结构(数据类型)
  override def dataType: DataType = DoubleType

  //官方用语：如果此函数是确定性的，则返回 true，即给定相同的输入，始终返回相同的输出。
  //人话：  聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
  override def deterministic: Boolean = true

  //中间层运算数据的初始化，buffer数据初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //buffer层数据已经结构化了，现在是对其进行赋值的初始化
    //这里的0，1，2，3，4.....就是对应bufferSchema里面的结构化数据的相应列
    buffer(0) = 0
    buffer(1) = 0
  }

  //运算：这里调用的是buffer里面的数据进行数据运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //运算方式类似于combine
    //组内:一条记录调用一次   buffer表示之前的数据(若没有则是初始化的数据)  input表示是新进来的数据
    buffer(0) = buffer.getInt(0) + input.getInt(0) //sum  老值=老值+新值
    buffer(1) = buffer.getInt(1) + 1 //count   老值=老值+1
  }

  //合并聚合缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //运算方式类似于combine
    //合并不同区里面的数据
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //计算最后的结果   值分别对应buffer里面的位数
  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0) / buffer.getInt(1)
  }
}
