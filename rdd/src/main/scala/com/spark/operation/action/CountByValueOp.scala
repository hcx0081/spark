package com.spark.operation.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountByValueOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CountByValueOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3)
    ), 2)
    
    val countByValueRdd: collection.Map[(String, Int), Long] = rdd.countByValue()
    
    countByValueRdd.foreach(println)
    // ((b,2),1)
    // ((a,2),1)
    // ((b,1),1)
    // ((a,1),1)
    // ((b,3),1)
    
    sc.stop()
  }
}
