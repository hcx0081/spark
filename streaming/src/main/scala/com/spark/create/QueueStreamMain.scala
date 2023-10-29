package com.spark.create

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object QueueStreamMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("QueueStreamMain")
    val sc = new StreamingContext(sparkConf, Seconds(5))
    
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    
    val ds: InputDStream[Int] = sc.queueStream(rddQueue)
    val m: DStream[(Int, Int)] = ds.map((_, 1))
    val r: DStream[(Int, Int)] = m.reduceByKey(_ + _)
    
    r.print()
    
    sc.start()
    
    for (i <- 1 to 5) {
      rddQueue += sc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(10000)
    }
    
    sc.awaitTermination()
  }
}
