package com.spark.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PersistMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("PersistMain")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map(word => {
      println("----")
      (word, 1)
    })
    
    // 进行RDD持久化
    wordOne.cache()
    
    val countByKeyRdd: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    countByKeyRdd.collect().foreach(println)
    
    val groupByKeyRdd: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    groupByKeyRdd.collect().foreach(println)
    
    sc.stop()
    
    // ----
    // ----
    // ----
    // ----
    // (Hello,2)
    // (World,1)
    // (Spark,1)
    // (Hello,CompactBuffer(1, 1))
    // (World,CompactBuffer(1))
    // (Spark,CompactBuffer(1))
  }
}
