package com.spark.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRdd {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    
    // 使用集合创建
    // val seq: Seq[Int] = Seq[Int](1, 2, 3, 4, 5)
    // val rdd: RDD[Int] = sc.parallelize(seq) // val rdd: RDD[Int] = sc.makeRDD(seq)
    
    // 使用外部文件创建
    val rdd: RDD[String] = sc.textFile("hello/src/main/resources/rdd.txt")
    
    rdd.collect().foreach(println)
    
    sc.stop()
  }
}
