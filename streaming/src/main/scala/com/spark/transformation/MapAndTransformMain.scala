package com.spark.transformation

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MapAndTransformMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapAndTransformMain")
    val sc = new StreamingContext(sparkConf, Seconds(5))
    
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    
    // code：执行位置：驱动器
    val wordOne: DStream[(String, Int)] = words.transform(rdd => {
      // code：执行位置：驱动器（收到数据之后并且周期执行）
      rdd.map(word => {
        // code：执行位置：执行器
        (word, 1)
      })
    })
    
    // code：执行位置：驱动器
    // val wordOne: DStream[(String, Int)] = words.map(word => {
    //   // code：执行位置：执行器
    //   (word, 1)
    // })
    
    // val wordCount: DStream[(String, Int)] = wordOne.reduceByKey(_ + _)
    //
    // wordCount.print()
    //
    // sc.start()
    //
    // sc.awaitTermination()
  }
}
