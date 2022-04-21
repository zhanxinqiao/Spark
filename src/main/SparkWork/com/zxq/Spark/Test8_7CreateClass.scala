package com.zxq.Spark
class BankAccount{
  private var balance=666
  def deposit: Int ={
    var i:Int=13
    println("deposit方法：返回值为:")
    return i
  }
  def withdraw(): Unit ={
    println("withdraw方法，没有返回值！")
  }

  def query=balance
}
object Test8_7CreateClass {
  def main(args: Array[String]): Unit = {
    val account: BankAccount = new BankAccount
    account.withdraw()
    var a=account.deposit
    println(a)
    println("只读属性为：",account.query)
  }
}
