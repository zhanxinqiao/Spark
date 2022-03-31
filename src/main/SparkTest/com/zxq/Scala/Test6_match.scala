package com.zxq.Scala

object Test6_match {
  def main(args: Array[String]): Unit = {
    val tup:(Double,Int,String,Boolean,Int)=(1.0,89,"abc",false,44)
    val iter:Iterator[Any]=tup.productIterator
    //这里并不是直接就迭代完了，而是调用一次，迭代一次
    val res:Iterator[Unit]=iter.map(
      (x)=>{
        //match 相当于Java里面的 switch case 语句
        //匹配数值 或者类型  没有匹配到就一直往下执行  直到结束
        x match {
          case 1 =>println(s"$x..is 1")
          case 88=>println(s"$x..is 88")
          case false=>println(s"$x...is false")
          case w:Int if w>40 =>println(s"$w..is >50 ")
          case _ =>println("不知道是什么类型")
        }
      }
    )
    while (res.hasNext)
      println(res.next())
  }

}
