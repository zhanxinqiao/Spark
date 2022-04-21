package com.zxq.Spark

import org.apache.calcite.linq4j.tree.Expressions.list

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
object Test9_8FullSort {
  def main(args: Array[String]): Unit = {
//    val seqToBuffer: Seq[List[Int]] => ListBuffer[List[Int]] = ListBuffer[List[Int]]
    list(5,6,7,8,9).permutations.map(x=>{
          var num=0
      breakable{
      for(y<-0 until x.size){
        if(x(y)==8 && y<2 && x(y+1)!=9){
          if(y==1) {
            if(x(y-1)!=9)
              break
          } else{
            break
          }}
        if(x(y)==8 && y>2 && x(y-1)!=9){
          if(y==3){
            if(x(y+1)!=9)
              break
          }else{
            break()
          }
        }
        if(x(2)==8){
          if(x(1)!=9&&x(3)!=9)
            break
        }
        if(x(2)==9){
          if(x(1)!=8&&x(3)!=8)
            break
        }
        num+=1
      }}
      if(num<5){
        x
      }else{
        List()
      }
    }).filter(x=>x.size>4).foreach(println(_))
  }

}
