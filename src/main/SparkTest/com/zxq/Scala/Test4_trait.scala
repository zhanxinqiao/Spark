package com.zxq.Scala
/*
* trait 相当于Java的类
* 用trait定义之后可用extends实现多个trait的继承
* */

trait God{
  def say():Unit={
    println("god...say")
  }
}

trait Mg{
  def ku():Unit={
    println("mg...say")
  }
  def haiRen():Unit
}
class Person(name:String) extends God with Mg{
  def hello():Unit={
    println(s"$name say hello")
  }
  //继承的trait里面未实现的方法，需要实现出来(这里和Java相同)
  override def haiRen(): Unit = {
        println("自己实现。。。。")
    }
}

object Test4_trait {
  def main(args: Array[String]): Unit = {
    val p = new Person("zhangsan")
    p.hello()
    p.say()
    p.ku()
    p.haiRen()
  }
}
