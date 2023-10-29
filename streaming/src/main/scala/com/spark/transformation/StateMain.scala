package com.spark.transformation

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SocketTextStreamMain")
    val sc = new StreamingContext(sparkConf, Seconds(5))
    
    /* 无状态 */
    // val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    // val words: DStream[String] = lines.flatMap(_.split(" "))
    // val wordOne: DStream[(String, Int)] = words.map((_, 1))
    // val wordCount: DStream[(String, Int)] = wordOne.reduceByKey(_ + _)
    
    /* 有状态 */
    sc.checkpoint("buff")
    
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordOne: DStream[(String, Int)] = words.map((_, 1))
    val wordCount: DStream[(String, Int)] = wordOne.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
      val newCount: Int = buff.getOrElse(0) + seq.sum
      Option(newCount)
    })
    
    
    wordCount.print()
    
    sc.start()
    
    sc.awaitTermination()
  }
}
