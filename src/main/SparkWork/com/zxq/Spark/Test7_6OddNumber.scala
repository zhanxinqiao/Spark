package com.zxq.Spark

object Test7_6OddNumber {
  def main(args: Array[String]): Unit = {
    var  sum=0
    for(i<-1 to 100 if i%2!=0){
      sum+=i
    }
    println(sum)
  }
}
