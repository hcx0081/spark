package com.spark.rdd.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("GlomOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    
    glomRdd.collect().foreach(item => println(item.mkString(", ")))
    // 1, 2
    // 3, 4, 5
  }
}
