package com.spark.shared.create.accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object CreateBroadcast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("CreateBroadcast")
    val sc = new SparkContext(sparkConf)
    
    val broadcast: Broadcast[Unit] = sc.broadcast()
    println(broadcast.value)
    
    sc.stop()
  }
}
