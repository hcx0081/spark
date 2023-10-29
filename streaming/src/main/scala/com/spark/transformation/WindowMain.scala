package com.spark.transformation

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WindowMain")
    val sc = new StreamingContext(sparkConf, Seconds(5))
    
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordOne: DStream[(String, Int)] = words.map((_, 1))
    
    val windowDs: DStream[(String, Int)] = wordOne.window(Seconds(10), Seconds(5))
    
    val wordCount: DStream[(String, Int)] = windowDs.reduceByKey(_ + _)
    
    wordCount.print()
    
    sc.start()
    
    sc.awaitTermination()
  }
}
