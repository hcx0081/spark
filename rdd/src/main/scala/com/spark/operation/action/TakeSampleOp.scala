package com.spark.operation.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TakeSampleOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TakeSampleOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val takeSampleRdd: Array[Int] = rdd.takeSample(false, 2)
    
    takeSampleRdd.foreach(println)
    // 2
    // 5
    
    sc.stop()
  }
}
