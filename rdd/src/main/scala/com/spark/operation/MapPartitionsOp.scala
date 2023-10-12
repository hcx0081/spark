package com.spark.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionsOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    
    val mapRdd: RDD[Int] = rdd.mapPartitions(iter => {
      println("====")
      iter.map(_ * 2)
    })
    
    mapRdd.collect().foreach(println)
    
    sc.stop()
  }
}
