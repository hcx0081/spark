package com.spark.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FoldByKeyOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3)
    ), 2)
    
    // val foldByKeyRdd: RDD[(String, Int)] = rdd.foldByKey()
    
    // foldByKeyRdd.foreach(println)
    
    sc.stop()
  }
}
