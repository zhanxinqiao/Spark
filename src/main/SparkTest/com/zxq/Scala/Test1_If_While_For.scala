package com.zxq.Scala

import scala.collection.immutable

//java中万物只有class    而在Scala中  除了类文件  还有object对象
object Test1_If_While_For {
  //流程控制语句
  def main(args:Array[String]): Unit ={
        //if
    println("-----------------if----------------")
    var a=0
    if (a<=0){
      println(s"$a<=0")
    }else{
      println(s"$a>0")
    }

    println("-------------while--------------------")
    var aa=0
    while (aa<10){
      println(aa)
      aa+=1
    }

    println("-------------for--------------------")
    /*
    * java写法： for(i=0;i<10;i++)
    *           for(P x:xs)
    *
    * scala写法：
    *   for(i<-1 to 9)
    *   for(i<-1 until(10))
    *   for(i<-1 to 9;j<-1 to 9 if(j<=i))  可以将多个for循环写在一起  并执行判断
    * */
    var seqs=1 until(10)
    println(seqs)
    //循环逻辑，业务逻辑
    for(i <- seqs if(i%2==0)){
      println(i)
    }
    println("---------九九乘法表方式一---------------")
    for (i<-1 to 9){
      for (j<-1 to 9){
        if(j<=i) print(s"$i * $j = ${i*j}\t")
        if(j==i) println()
      }
    }

    println("---------九九乘法表方式二---------------")
    for(i<-1 to 9;j<-1 to 9 if(j<=i)){
      if (j<=i) print(s"$i * $j =${i*j}\t")
      if(j == i) println()
    }

    println("---------for高级用法---------------")
    //定义一个Vector数组  并在定义时进行相应的操作
    val seqss: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield {
      var x = 8
      i+x
    }
    println(seqss)
    for(i<- seqss){
      println(i)
    }
      }
}
