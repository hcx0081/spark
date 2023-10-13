package com.spark.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SampleOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SampleOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val sampleRdd: RDD[Int] = rdd.sample(true, .5)
    
    sampleRdd.collect().foreach(println)
    // 2
    // 2
    // 4
    
    sc.stop()
  }
}
