package com.spark.shared.create.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCountByAcc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountByAcc")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "World", "Hello", "Spark"))
    
    val wcAcc = new MyAccumulator
    sc.register(wcAcc)
    
    rdd.foreach(word => {
      wcAcc.add(word)
    })
    println(wcAcc.value) // Map(Hello -> 2, Spark -> 1, World -> 1)
    
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {
  private var wcMap = mutable.Map[String, Int]()
  
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAccumulator
  
  override def reset(): Unit = wcMap.clear()
  
  override def add(word: String): Unit = {
    val wordCount: Int = wcMap.getOrElse(word, 0) + 1
    wcMap.update(word, wordCount)
  }
  
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    var map1 = this.value
    var map2 = other.value
    
    map2.foreach {
      case (word, count) => {
        val newCount: Int = map1.getOrElse(word, 0) + count
        map1.update(word, newCount)
      }
    }
  }
  
  override def value: mutable.Map[String, Int] = wcMap
  
  override def isZero: Boolean = wcMap.isEmpty
}