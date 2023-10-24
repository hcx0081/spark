package com.spark.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object HelloMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HelloMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 引入隐式转换规则
    
    val data: List[Int] = List(1, 2, 3, 4, 5)
    val ds: Dataset[Int] = data.toDS()
    ds.show()
    // +-----+
    // |value|
    // +-----+
    // |    1|
    // |    2|
    // |    3|
    // |    4|
    // |    5|
    // +-----+
    
    spark.stop()
  }
}
