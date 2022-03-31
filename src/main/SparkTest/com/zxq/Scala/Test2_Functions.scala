package com.zxq.Scala

import sun.util.calendar.BaseCalendar
import java.util
import java.util.Date

object Test2_Functions {
  def main(args: Array[String]): Unit = {
    // 方法  函数
    println("--------------1.basic--------------")

    def fun01(): Unit = {
      println("hello world")
    }

    fun01()
    var x = 3
    //返回值，参数，函数体
    //当函数无返回值时,调用函数接收的对象为空  若强制性的指定数据类型，会报错
    val y = fun01()
    println(y + "----")

    //想有返回
    //有return必须给出返回类型
    def fun02(): Unit = {
      new util.LinkedList[String]()
    }

    //参数：必须给出类型，是val
    //class 构造，是var，val
    def fun03(a: Int): Unit = {
      println(a)
    }

    fun03(33)

    println("----------------2.递归函数-----------------")

    def fun04(num: Int): Int = {
      if (num == 1) {
        num
      } else {
        num * fun04(num - 1)
      }
    }

    val i: Int = fun04(4)
    println(i)

    println("----------------3.默认值函数-----------------")

    def fun05(a: Int = 8, b: String = "abc") {
      println(s"$a\t$b")
    }

    fun05(9, "def")
    fun05(22)
    fun05(b = "ooxx")

    println("----------------4.匿名函数-----------------")
    /*
  * 函数是第一类值
  * 函数：
  * 1.签名：（Int,Int）=>Int: 参数类型列表 => 返回值类型
  * 2.匿名函数: (a:Int,b:Int) => {a+b} :(参数实现列表) =>函数体
  * */
    var xx: Int = 3
    // 匿名函数
    var yy: (Int, Int) => Int = (a: Int, b: Int) => {
      a + b
    }
    var w: Int = yy(3, 4)
    println(w)

    println("----------------5.嵌套函数-----------------")

    def fun06(a: String): Unit = {
      def fun05: Unit = {
        println(a)
      }
      //这里调用的时fun06里面的的fun05
      fun05
    }

    fun06("hello")

    println("----------------6.偏应用函数-----------------")

    def fun07(date: Date, tp: String, msg: String): Unit = {
      println(s"$date\t$tp\t$msg")
    }
    //调用时传三个参数
    fun07(new Date(), "info", "ok")
    //调用函数将其修改为固定值函数后赋值给变量，以达到解耦的目的
    var info = fun07(_: Date, "info", _: String)
    var error = fun07(_: Date, "error", _: String)

    info(new Date, "ok")
    error(new Date, "error...")

    println("----------------7.可变参数-----------------")
    //加星号代表有一个或者多个
    def fun08(a: Int*): Unit = {
      println("----1----------")
      for (elem <- a) {
        println(elem)
      }
      println("----2----------")
      //foreach  迭代器式的遍历
      a.foreach(println)
    }

    fun08(2)
    fun08(1, 2, 3, 4, 5, 6)

    println("----------------8.高阶函数-----------------")
    //函数作为参数  函数作为返回值

    //函数作为参数(即参数中定义了函数)
    def computer(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
      //实现了一个函数，计算不同的类型的值，简化了代码
      val res: Int = f(a, b)
      println(res)
    }

    computer(3, 8, (x: Int, y: Int) => {
      x + y
    })

    computer(3, 8, (x: Int, y: Int) => {
      x * y
    })
    //调用乘集那一个的用法
    computer(3, 8, _ * _)

    //函数作为返回值：(即函数后参数列表外的返回值类型(也就是冒号后面的值 是一个函数式(给出输入输出参数  和返回值类型)))
    //函数作为返回值  通常会被执行完这个函数
    def factory(i: String): (Int, Int) => Int = {
      def plus(x: Int, y: Int): Int = {
        x + y
      }
      if (i.equals("+")){
        plus
      }else{
        (x:Int,y:Int)=>{
          x*y
        }
      }
    }
    //调用函数作为参数的函数，这个参数函数，又恰好是函数作为返回值的函数数
    computer(3,8,factory("-"))

    println("----------------9.柯里化-----------------")
    //即将函数后面用多个括号隔开赋值，可达到动态参数类型、动态参数数量的效果
    def fun09(a:Int)(b:Int)(c:String): Unit ={
      println(s"$a\t$b\t$c")
    }
    fun09(3)(8)("sdfgh")

    def fun10(a:Int*)(b:String*): Unit ={
      a.foreach(println)
      b.foreach(println)
    }

    fun10(1,2,3)("asd","asdf","wefg")

    println("----------------*.方法-----------------")
    //方法不想执行，赋值给一个引用   方法名+空格+下划线
    val funa=println
    println(funa)
    val func=println _
    func
    //语法 -> 编译器 -> 字节码 <- jvm规则
    //编译器衔接人和机器
    //Java中 + :关键字
    //scala 中+:  方法/函数
    //Scala语法中，没有基本类型，所以你写一个数字 3 编译器/语法，其实是把3看待成了Int这个对象
    //3+2       <=> 3.+(2) <=>  3:Int

  }
}
