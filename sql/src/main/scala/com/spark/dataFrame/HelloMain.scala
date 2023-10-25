package com.spark.dataFrame

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object HelloMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HelloMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
    val df: DataFrame = spark.read.json("data-sql/user.json")
    
    // df.show()
    // +---+--------+
    // |age|username|
    // +---+--------+
    // | 20|      zs|
    // | 20|      ls|
    // | 20|      ww|
    // +---+--------+
    
    /* SQL */
    df.createOrReplaceTempView("user")
    
    // spark.sql("select * from user").show()
    // +---+--------+
    // |age|username|
    // +---+--------+
    // | 20|      zs|
    // | 20|      ls|
    // | 20|      ww|
    // +---+--------+
    
    // spark.sql("select age, username from user").show()
    // +---+--------+
    // |age|username|
    // +---+--------+
    // | 20|      zs|
    // | 20|      ls|
    // | 20|      ww|
    // +---+--------+
    
    // spark.sql("select avg(age) from user").show()
    // +--------+
    // |avg(age)|
    // +--------+
    // |    20.0|
    // +--------+
    
    /* DSL */
    // df.select("age", "username").show()
    // +---+--------+
    // |age|username|
    // +---+--------+
    // | 20|      zs|
    // | 20|      ls|
    // | 20|      ww|
    // +---+--------+
    
    import spark.implicits._ // 引入隐式转换规则
    df.select($"age" + 10).show()
    df.select('age + 10).show()
    // +----------+
    // |(age + 10)|
    // +----------+
    // |        30|
    // |        30|
    // |        30|
    // +----------+
    
    spark.stop()
  }
}
