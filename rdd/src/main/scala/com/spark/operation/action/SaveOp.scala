package com.spark.operation.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SaveOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SaveOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3)
      ), 2)
    
    rdd.saveAsTextFile("outputSaveAsTextFile")
    rdd.saveAsObjectFile("outputSaveAsObjectFile")
    rdd.saveAsSequenceFile("outputSaveAsSequenceFile")
    
    sc.stop()
  }
}
