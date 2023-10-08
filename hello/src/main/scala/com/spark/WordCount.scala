package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    
    // 读取文件，获取一行一行的数据
    /*
    * hello world
    * hello world
    *  */
    val lines: RDD[String] = sc.textFile("hello/src/main/resources/1.txt")
    
    // 拆分一行一行的数据，形成一个一个的单词（分词）
    /*
    * hello world => hello, world
    * hello world => hello, world
    *  */
    val words: RDD[String] = lines.flatMap(line => line.split(" "))
    
    // 根据单词进行分组
    /*
    * (hello, hello), (world, world)
    *  */
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    
    // 聚合分组之后的数据
    /*
    * (hello, 2), (world, 2)
    *  */
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) =>
        (word, list.size)
    }
    
    // 输出结果
    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)
    
    sc.stop()
  }
}
