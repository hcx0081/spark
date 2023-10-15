package com.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    
    // 读取文件，获取一行一行的数据
    /*
    * hello world
    * hello world
    *  */
    val lines: RDD[String] = sc.textFile("hello/src/main/resources/hello.txt")
    
    // 拆分一行一行的数据，形成一个一个的单词（分词）
    /*
    * hello world => hello, world
    * hello world => hello, world
    *  */
    val words: RDD[String] = lines.flatMap(line => line.split(" "))
    
    // 分词之后统计各个单词数量
    /*
    * hello world => (hello, 1), (world, 1)
    * hello world => (hello, 1), (world, 1)
    *  */
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    
    // 根据单词进行分组
    /*
    * (hello, ((hello, 1), (hello, 1))), (world, ((world, 1), (world, 1)))
    *  */
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    
    // 聚合分组之后的数据
    /*
    * (hello, 2), (world, 2)
    *  */
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      }
    }
    
    // 输出结果
    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)
    // (hello,2)
    // (world,2)
    
    sc.stop()
  }
}
