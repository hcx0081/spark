package com.spark.to

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RddAndDataFrame {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RddAndDataFrame")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // import spark.implicits._ // 引入隐式转换规则
    
    /* RDD => DataFrame */
    // val rdd: RDD[(String, Int)] = sc.makeRDD(Seq(("zs", 20), ("ls", 20), ("ww", 20)))
    // val df: DataFrame = rdd.toDF()
    // val df: DataFrame = spark.createDataFrame(rdd)
    //
    // df.show()
    
    /* DataFrame => RDD */
    val df: DataFrame = spark.createDataFrame(Seq(("zs", 20), ("ls", 20), ("ww", 20)))
    val rdd: RDD[Row] = df.rdd
    
    rdd.collect().foreach(println)
    
    spark.stop()
  }
}
