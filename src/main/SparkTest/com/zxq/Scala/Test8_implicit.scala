package com.zxq.Scala
import java.util
object Test8_implicit {
  def main(args: Array[String]): Unit = {
    val listLinked=new util.LinkedList[Int]()
    listLinked.add(1)
    listLinked.add(2)
    listLinked.add(3)
    val listArray = new util.ArrayList[Int]()
    listArray.add(1)
    listArray.add(2)
    listArray.add(3)

    /*
    * list.foreach(println)  //3个东西： lsit数据集  foreach 遍历行为    println处理函数
    *
    * def foreach[T](list:util.LinkedList[T],f:(T):Unit):Unit={
    *    val iter:unit.Iterator[T] = list.iterator()
    *    while(iter.hasNext)  f(iter.next())
    * }
    *
    *
    foreach(list,println)
    val xx=new XXX(list)
    xx.foreach(println)
    * */

    //隐式转换：隐式转换方法
    implicit def sdfsdf[T](list:util.LinkedList[T])={
      val iter:util.Iterator[T]=list.iterator()
      new XXX(iter)
    }
    implicit def sdfsdfsdf[T](list:util.ArrayList[T])={
      val iter:util.Iterator[T]=list.iterator()
      new XXX(iter)
    }
    //隐式转换  即调用自己不存在的方法时  编译器会自动去寻找implicit定义的方法(函数)
    //并且对参数进行对比  看是否相同  若相同则调用这个方法(函数)
    listLinked.foreach(println)
    listArray.foreach(println)

    //list.foreach(println):
    //必须先承认一件事情:list有foreach方法吗？  肯定是没有的
    /*
     1.scala编译器发现list.foreach(println) 有bug
     2.去寻找有没有implicit  定义的方法(函数)，且方法(函数)的参数正好是list的类型！ ！ ！
     3.编译器：完成你曾经人类：
     val xx=new XXX(list)
     xx.foreach(println)
     编译器帮你把代码改了
    */
    //同时在柯里化里使用implicit时  如果有定义了相同类型的implicit变量时，可间接性当作默认参数使用
    implicit val sdfsdfsd:String="werwe"
    implicit val ssss:Int=88
    def ooxx(age:Int)(implicit name:String): Unit ={
      println(name+" "+age)
    }
    ooxx(66)("jkljkl")
    ooxx(66)

  }

}

class XXX[T](iter: util.Iterator[T]){
  def foreach(f:(T)=>Unit): Unit ={
    while (iter.hasNext)
      f(iter.next())
  }
//  case class XXX[T](iter: util.Iterator[T]){
//    def foreach(f:(T)=>Unit): Unit ={
  //   val iter:util.Iterator[T]=list.iterator()
//      while (iter.hasNext)
//        f(iter.next())
//    }
}
