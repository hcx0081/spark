package com.spark.rdd.operation.transformation.pair.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CogroupOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 5)), 2)
    
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    
    cogroupRdd.collect().foreach(println)
    // (b,(CompactBuffer(2),CompactBuffer(5)))
    // (a,(CompactBuffer(1, 3),CompactBuffer(4)))
    // (c,(CompactBuffer(),CompactBuffer(5)))
    
    sc.stop()
  }
}
