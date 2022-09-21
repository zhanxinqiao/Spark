package com.zxq.Streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object Test03_DStream_API {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testAPI")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(".")    //设置RDD将被检查点的目录
    val ssc: StreamingContext = new StreamingContext(sc, Duration(1000))  //数据源切分的时候，设置最小粒度 Duration：流数据将被分批的时间间隔

//    ---------------------window api-------------------
   /*          window机制中(时间定义大小)
   *     ①窗口大小：计算量，去多少batch
   *     ②步进，滑动距离，job启动间隔
   * */
//   val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
//   val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x => (x(0), x(1).toInt))

    //将接收到的数据   以每秒的间隔打印输出
//    val res1s1batch: DStream[(String, Int)] = format.reduceByKey(_ + _)
//    res1s1batch.mapPartitions(iter=>{println("1s");iter}).print()

    //将接收到的数据   以5秒的间隔打印输出  实际是一秒一输出  统计当前秒往前的5秒的和(最新的5个batch)，不足5秒也照样输出
//    val newDS: DStream[(String, Int)] = format.window(Duration(5000))
//    val res5s5batch: DStream[(String, Int)] = newDS.reduceByKey(_ + _)
//    res5s5batch.mapPartitions(iter=>{println("5s");iter})print()//打印频率   想要：5秒打印一次         实际：1秒打印一次

    //每秒钟看到历史5秒的统计
//    val res: DStream[(String, Int)] = format.reduceByKey(_ + _)  //只对当前秒进行统计  所以永远就是 hello:1 hi:2
//    val res: DStream[(String, Int)] = format.window(Duration(5000)).reduceByKey(_ + _)  //拿出5秒的批次，以最小粒度为步长(这里是1s)


//     val reduce: DStream[(String, Int)] = format.reduceByKey(_ + _)  //窗口量是1000  slide 1000
//     val res: DStream[(String, Int)] = reduce.window(Duration(5000))  //调整每次我们可以看到的窗口量
     //上述的这种方法我们每次看都是消息条数逐渐累加，直至十条趋于稳定，不是我们想要的统计

    //改进版
//    val win: DStream[(String, Int)] = format.window(Duration(5000))   //先调整量
//    val res: DStream[(String, Int)] = win.reduceByKey(_ + _)      //再基于上一步的量上整体发生计算

    //高级版
//    val res: DStream[(String, Int)] = format.reduceByKeyAndWindow(_ + _,Duration(5000))

//     res.print()

    /*
    * foreachRDD:末端处理
    *    (rdd)=>{
    *       rdd.foreach(x=>{
    *          x..to redis
    *          x..to mysql
    *         call webservice
    *       })
    *      }
    *
    *
    * transform  中途加工
    *    val res: DStream[(String, Int)] = format.transform(  //硬性要求：返回值是RDD
    *   (rdd) => {
    *    rdd.foreach(println);  //产生job
    *    val rddres: RDD[(String, Int)] = rdd.map(x => (x._1, x._2 * 10))  //只是在做转换
    *    rddres
    *  })
    * */



    //--------------------------------------DSteam 转换到RDD 及 代码作用域----------------------------------
     /*
     * 转换到RDD的操作
     *   有两种途径  :  ①末端处理  ②中途加工
     *
     * 重点是作用域
     *        作用域分为三个级别:
     *              ①application
     *              ②job
     *              ③rdd : task
     *
     *        RDD是一个单向链表  DStream也是一个单向链表
     *        如果把最后一个DStream给SSC  那么SSC可以启动一个单独的线程 无while(true){最后一个DStream遍历}
     * */
//
//    var bc: Broadcast[List[Int]] = sc.broadcast((1 to 5).toList)
//    var jobNum=0
//    println("aaaaaa")  //application级别
    // 筛选前五条
//    val res: DStream[(String, Int)] = format.filter(x => {
//      bc.value.contains(x._2)
//    })


//  val res: DStream[(String, Int)] = format.transform(rdd => {
//  jobNum += 1  //每job级别递增，是在ssc的另一个while(true)线程里  Driver端执行的
//  println(s"jobNum:$jobNum")
//  if (jobNum <= 5) {
//    bc = sc.broadcast((1 to 5).toList)
//  } else {
//    bc = sc.broadcast((6 to 15).toList)
//  }
//  rdd.filter(x => bc.value.contains(x._2))  //无论多少次job的运行都是相同的bc  只有RDD接受的函数，才是executor端的，才是task端的
//})

//    val res: DStream[(String, Int)] = format.transform(
//      //每job级别调用一次
//      (rdd) => {
//        //我们函数是每job级别的
//        println("bbbbbbbb")    // job级别
//        rdd.map(x => {
//          println("ccccccccc")   //task级别
//          x
//        })
//      }
//    )


    //--------------------------------------有状态计算 ----------------------------------
    /*
    * 状态 <- 历史数据  join、 关联  历史的计算要存下来、当前的计算最后还要合到历史数据里
    * 持久化下来 历史的数据状态
    * persist       blockmanager(本地文件块)   速度快    可靠性差
    * checpoint     外界系统                  成本高     可靠性好
    *    ****  persist  调用后  再做  checkpoin  => 数据会在两个地方都存储
    * */

        val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
        val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x => (x(0), 1))

    //①  这里运用reduceBykeyAndWindow 来计算批次的移动，保证指定窗口容量里面的批次量没有计算错误
//        val res: DStream[(String, Int)] = format.reduceByKeyAndWindow(
//          //计算重新进入的batch的数据
//          (ov: Int, nv: Int) => {
//            println("first fun ......")
//            println(s"ov:$ov  nv:$nv")
//            ov + nv
//          },
//          //挤出去的batch的数据
//          (ov: Int, oov: Int) => {
//            println("di 2 ge fun....")
//            println(s"ov:$ov   oov:$oov")
//            ov - oov
//          },
//          Duration(6000), //窗口容量
//          Duration(2000) //步进的容量
//        )

    // ② 这里运用 updateStateByKey 进行窗口计算
//    val res: DStream[(String, Int)] = format.updateStateByKey(
//      (nv: Seq[Int], ov: Option[Int]) => {
//        println("...updata...fun")
//        //每个批次的job里  对着nv求和
//        val count: Int = nv.count(_ > 0)
//        val oldVal: Int = ov.getOrElse(0)
//        Some(count + oldVal)
//      }
//    )

    // ③这里运用 mapWithState 进行窗口计算
    val res: MapWithStateDStream[String, Int, Int, (String, Int)] = format.mapWithState(StateSpec.function(
      (k: String, nv: Option[Int], ov: State[Int]) => {
        println(s"**************k:$k    nv:${nv.getOrElse(0)}  ov:${ov.getOption().getOrElse(0)}")
        (k, nv.getOrElse(0) + ov.getOption().getOrElse(0))
      }
    ))



    res.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
