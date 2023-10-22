package com.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountMain")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    wordCountCountByValue(sc, rdd)
  }
  
  def wordCountCountByValue(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountCountByKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountCombineByKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountFoldByKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountAggregateByKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountReduceByKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountGroupByKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordGroup: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = wordGroup.mapValues(group => group.size)
    wordCount.foreach(println)
    sc.stop()
  }
  
  def wordCountMapValues(sc: SparkContext, rdd: RDD[String]): Unit = {
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = wordGroup.mapValues(group => group.size)
    wordCount.foreach(println)
    sc.stop()
  }
}
