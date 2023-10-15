package com.spark.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKeyOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1)
    ), 2)
    
    val reduceByKeyRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    
    reduceByKeyRdd.foreach(println)
    // (a,2)
    // (b,3)
    
    sc.stop()
  }
}
