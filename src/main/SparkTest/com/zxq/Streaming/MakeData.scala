package com.zxq.Streaming

import java.io.{OutputStream, PrintStream}
import java.net.{ServerSocket, Socket}

object MakeData {
  def main(args: Array[String]): Unit = {
    val listen: ServerSocket = new ServerSocket(8888)
    println("server start")
    while (true){
      val socket: Socket = listen.accept()
      new Thread(){
        override def run(): Unit = {
          var num=0
          if(socket.isConnected){
            val out: OutputStream = socket.getOutputStream
            val printer: PrintStream = new PrintStream(out)
            while(socket.isConnected){
              num +=1

              printer.println(s"hello ${num}")
              printer.println(s"hi ${num}")
              printer.println(s"hi ${num}")

              Thread.sleep(1000)
            }
          }
        }
      }.start()

    }
  }

}
