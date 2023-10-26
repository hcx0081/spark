package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountMain")
    val sc = new StreamingContext(sparkConf, Seconds(5))
    
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordOne: DStream[(String, Int)] = words.map((_, 1))
    val wordCount: DStream[(String, Int)] = wordOne.reduceByKey(_ + _)
    
    wordCount.print()
    
    // 不可直接停止
    // sc.stop()
    
    sc.start()
    sc.awaitTermination()
  }
}
