package com.zxq.Scala

case class Dog(name:String,age:Int){}
object Test5_case_class {
  def main(args: Array[String]): Unit = {
    val dog1=Dog("hsq",18)
    val dog2=Dog("hsq",18)
    //和Java中不同，此处实现的同一个case class中若内容相同  则equals  ==  比对结果为True
    println(dog1.equals(dog2))
    println(dog1==dog2)
  }
}
