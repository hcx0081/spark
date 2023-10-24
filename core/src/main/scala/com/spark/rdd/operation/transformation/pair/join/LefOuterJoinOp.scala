package com.spark.rdd.operation.transformation.pair.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LefOuterJoinOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("LefOuterJoinOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 5)), 2)
    
    val leftOuterJoinRdd: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    
    leftOuterJoinRdd.collect().foreach(println)
    // (b,(2,Some(5)))
    // (a,(1,Some(4)))
    // (a,(3,Some(4)))
    
    sc.stop()
  }
}
