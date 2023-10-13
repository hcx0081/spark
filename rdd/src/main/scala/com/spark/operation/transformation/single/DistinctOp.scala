package com.spark.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistinctOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DistinctOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 2, 1), 2)
    
    val filterRdd: RDD[Int] = rdd.distinct()
    
    filterRdd.collect().foreach(println)
    // 2
    // 1
    // 3
    
    sc.stop()
  }
}
