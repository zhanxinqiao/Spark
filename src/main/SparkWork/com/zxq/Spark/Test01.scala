package com.zxq.Spark

//Scala实现回文数
object Test01 {
  //方法一  用字符串
    def isHWS(x:Int):Boolean={
        if(x<0)
          return false
        if(0<=x&&x<=9)
          return true
        val s=x.toString
        for(i <-0 to  s.length/2){
         if(s(i)!=s(s.length-1-i))
           return false
       }
      return true
    }
  //方法二  用数字计算
//  def isHWS(x:Int):Boolean={
//    if(x<0)
//      return false
//    if(0<=x&&x<=9)
//      return true
//    var z=x
//    var q=0
//    while (z!=0){
//      q=q*10+(z%10)
//      z =z/10
//    }
//    return q==x
//  }

  def main(args: Array[String]): Unit = {
      var x=12320
      println(isHWS(x))
  }
}
