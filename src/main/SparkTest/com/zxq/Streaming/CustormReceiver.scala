package com.zxq.Streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

class CustormReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.DISK_ONLY) {
  override def onStart(): Unit = {
    new Thread {
      override def run(): Unit = {
        sock()
      }
    }.start()
  }

  private  def sock():Unit={
    val server: Socket = new Socket(host, port)
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(server.getInputStream))
    var line: String = reader.readLine()
    while (!isStopped()&&line!=null){
      store(line)
      line=reader.readLine()
    }
  }

  override def onStop(): Unit = ???
}