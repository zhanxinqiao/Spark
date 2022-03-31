package com.zxq.Scala

object Test7_PartialFunction {
  def main(args:Array[String]):Unit={
    //偏函数的应用  相当于match
    def xxx:PartialFunction[Any,String]={
      case "hello"=>"val is hello"
      case x:Int=>s"$x..is Int"
      case _=> "none"
    }
    println(xxx(44))
    println(xxx("hello"))
    println(xxx("hi"))
  }

}
