package com.spark.create

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object ReceiverStreamMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiverMain")
    val sc = new StreamingContext(sparkConf, Seconds(5))
    
    val ds: ReceiverInputDStream[Int] = sc.receiverStream(new MyReceiver)
    val m: DStream[(Int, Int)] = ds.map((_, 1))
    val r: DStream[(Int, Int)] = m.reduceByKey(_ + _)
    
    r.print()
    
    sc.start()
    
    sc.awaitTermination()
  }
}

class MyReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) {
  private var flag = true
  
  override def onStart(): Unit = {
    new Thread(() => {
      while (flag) {
        val mes: Int = new Random().nextInt(10)
        
        store(mes)
        
        Thread.sleep(2000)
      }
    }).start()
  }
  
  override def onStop(): Unit = {
    flag = false
  }
}