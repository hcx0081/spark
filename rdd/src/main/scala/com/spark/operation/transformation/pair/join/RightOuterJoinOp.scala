package com.spark.operation.transformation.pair.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RightOuterJoinOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RightOuterJoinOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 5)), 2)
    
    val rightOuterJoinRdd: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    
    rightOuterJoinRdd.collect().foreach(println)
    // (b,(Some(2),5))
    // (a,(Some(1),4))
    // (a,(Some(3),4))
    // (c,(None,5))
    
    sc.stop()
  }
}
