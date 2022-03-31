package com.zxq.Scala
/*
* 类里 ，裸露的代码是默认构造中的。有默认构造
* 只有在类名主构造器其中的参数(即类名后面括号内的参数）可以设置成var,且是private类型
* 其他方法函数中或者辅助构造器的参数都是val类型的,且不允许设置成var类型
*
* */
class Test0_Object_Class(var sex:Any) {//主构造器
//  println(sex)
    /*
  使用主构造器  传递的参数可被本身调用执行，
  val test1 = new Test0("----------")
  使用辅助构造器 调用的参数是辅助构造器里面传递给主构造器的参数（即this函数的this方法）
  val test = new Test0(33)

*/
  println(sex)
  var name="class:zhangsan"

  //主 构造器相当于Java的类   辅助构造器相当于Java的构造方法，根据参数类型，参数数量的不同区分（故必须是val类型）
  def this(xname:Int){//辅助构造器
    this("abc")
    println(sex+"-----------")
    println(xname)
    var a=33
    println(a)
  }

 def this(xsex:Double){
   this("def")
   println()
 }
  var a:Int=3

  def shuchu(): Unit ={
    print("被调用！")
  }

  println(s"ooxx....up$a....")

  def printMsg(): Unit={
    println(s"sex: ${Test0_Object_Class.name}")
    val test = new Test0_Object_Class(33)
    test.shuchu()
    val test1 = new Test0_Object_Class("---kk-------")
    test1.shuchu()
  }
  println(s"ooxx...up${a+4}")
}
//类名和对象名相同的  叫伴生类
//和Java里面的 static 单例对象非常相似
object Test0_Object_Class{
    //这里new的对象，会执行裸露代码，和对象内的方法
   //可根据参数的不同，选则辅助的构造器（也就是构造方法，他会先进去执行this方法，将主构造器需要的参数传递，便于调用），
   // 然后先执行主构造器类（也就是类）的裸露代码（不在任何函数或者辅助构造器之内的代码），然后再执行辅助构造器内的代码
   private val xo=new Test0_Object_Class(11.2)

   private val name="object:zhangsan"
   println("ooxx....up")

  def main(args: Array[String]): Unit = {
    println("hello from ooxx")
    //这里调用的方法，默认将对象本身的自定义参数传递过去了，故在printMsg()中调用和对象相同名字的类时用的参数值来自对象
    xo.printMsg()
  }

  println("ooxx...down")
}

