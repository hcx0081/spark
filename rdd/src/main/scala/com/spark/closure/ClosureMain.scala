package com.spark.closure

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ClosureMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ClosureMain")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val search: Search = new Search("H")
    
    // search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)
    
    sc.stop()
  }
  
  
  class Search(query: String) {
    // 函数序列化
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }
    
    def isMatch(str: String): Boolean = {
      str.contains(query)
    }
    
    // 属性序列化
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}
