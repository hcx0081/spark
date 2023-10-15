package com.spark.operation.transformation.pair.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 5)), 2)
    
    val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    
    joinRdd.collect().foreach(println)
    // (b,(2,5))
    // (a,(1,4))
    // (a,(3,4))
    
    sc.stop()
  }
}
