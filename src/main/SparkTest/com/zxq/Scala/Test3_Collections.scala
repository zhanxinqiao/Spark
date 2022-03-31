package com.zxq.Scala
import java.util
import scala.collection.mutable
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
object Test3_Collections {
      def main(args:Array[String]): Unit ={
          val listjava=new util.LinkedList[String]
          listjava.add("hello")

          //scala还有自己的collections
        println("--------------1.数组-------------------")
        /*
        * 1.数组
        * Java泛型是<>  scala中的是[]  所以数组用(n)
        * val 约等于 final  不可变描述的是val指定引用的值(值：字面值，地址)  这个的对象的状态是可以改变的
        * */
        val arr01=Array[Int](1,2,3,4)
        arr01(1)=99
        println(arr01(0))

        for (elem <- arr01) {
          println(elem)
        }

        println("--------------2.list-------------------")
        //链表
        //Scala中collections中有两个包：immutable,mutable  默认是不可变的immutable
        val list01=List[Int](1,2,3,4,5,4,3,2,1)
        for (elem <- list01) {
          println(elem)
        }
        list01.foreach(println)

        println("------------list02------------------")
        //这个的对象的状态是可以改变的  因为给的是可变参数类型ListBuffer
        val list02=new ListBuffer[Int]()
        list02.+=(33)
        list02.+=(34)
        list02.+=(35)
        list02.foreach(println)

        println("--------------3.Set------------------")
        val set01:Set[Int]=Set(1,2,3,4,2,1)
        for (elem <- set01) {
          println(elem)
        }
        set01.foreach(println)
        println("---------------")
        //定义可变val类型
        val set02:mutable.Set[Int]=Set(11,22,33,44,11)
        set02.add(88)

        set02.foreach(println)
        println("---------------")
        //定义不可变val类型  此时没有add()方法
        val set03:Predef.Set[Int]=scala.collection.immutable.Set(33,44,22,11)

        println("--------------3.tuple------------------")
        //val t2=new Tuple2(11,"sdfggfd) //2元素的Tuple2  在Scala描绘的是K，V
        val t2=(11,"sdfds")  //2元素的Tuple2  在Scala描绘的是K，V
        val t3=Tuple3(22,"sdfg",'w')
        val t4:(Int,Int,Int,Int)=(1,2,3,4)
        //最长22个
//        val t22 :Unit= (Int,Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)= ((a:Int, b:Int)=>a+b+8,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22)
        println(t2._1)
        println(t3._3)

        println("--------------4.map------------------")
        //val 默认都是immutable类型  即不可变
        val map01:Map[String,Int]=Map(("a",33),"b"->22,("c",3434),("a",3333))
        val keys:Iterable[String]=map01.keys
        val values:Iterable[Int]=map01.values
        //获取到值
        println(map01.get("a").get)
        println(map01.get("a").getOrElse("hello world"))
        println(map01.get("w").getOrElse("hello world"))
        for (elem <- keys) {
          println(s"key:$elem    values: ${map01.get(elem).get}")
        }
        keys.foreach(println)
        values.foreach(println)
        //定义mutable类型的val类型
        val map02:mutable.Map[String,Int]=scala.collection.mutable.Map(("a",11),("b",22))
        map02.put("c",22)

        println("-----------------艺术操作------------------")
        //定义val不可变类型的List数组list
        val list=List(1,2,3,4,5,6)
        list.foreach(println)
        println("---------------")

        //将list数组进行相关的操作  这个的对象的状态是可以改变的  因为给的是可变参数类型List
        val listMap:List[Int]=list.map(_+10)
        listMap.foreach(println)
        println("---------------")

        val listMap02:List[Int]=list.map(_*10)
        listMap02.foreach(println)

        println("-----------------艺术操作升华------------------")
        val listStr=List(
          "hello world",
          "hello mab",
          "good idea"
        )
        val flatMap=listStr.flatMap((x:String)=>x.split(" "))
        flatMap.foreach(println)
        val maplist=flatMap.map((_,1))
        maplist.foreach(println)
        //以上操作内存扩大了N倍  每一步计算内存都留有对象数据；
        //可以用Iterator来解决数据计算中间状态占用内存这一个问题

        println("-----------------艺术操作再升华------------------")
        //基于迭代器的源码分析
        val iter:Iterator[String]=listStr.iterator //迭代器是纯指针的操作，不存数据
        //以下操作都是迭代器之间的相互调用及查看  最终结果保存在缓冲区 缓冲区数据即最终数据到iterMapList
        val iterFlatmap=iter.flatMap((x:String)=>x.split(" "))
        //这个时候是迭代器迭代的时候，如果运用下面一行代码，即迭代器遍历了堆后那么就不会有iterMapList
//        iterFlatmap.foreach(println)
        println("---------------")

        val iterMapList=iterFlatmap.map((_,1))
        while (iterMapList.hasNext){
          val tuple:(String,Int)=iterMapList.next()
          println(tuple)
        }
//        iterFlatmap.foreach(println)

        /**总结：
        *    1.listStr是真正的数据集，有数据的
         *    iter.flatMap  没有发生计算，返回了一个新的迭代器
         * */
      }
}
