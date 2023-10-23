package com.spark.to

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameAndDataset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RddAndDataFrame")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 引入隐式转换规则
    
    /* DataFrame => Dataset */
    val df: DataFrame = Seq(("zs", 20), ("ls", 20), ("ww", 20)).toDF("name", "age")
    val ds: Dataset[(String, Int)] = df.select("name", "age").as[(String, Int)]
    ds.show()
    
    /* Dataset => DataFrame */
    val df: DataFrame = Seq(("zs", 20), ("ls", 20), ("ww", 20)).toDF("name", "age")
    val ds: Dataset[(String, Int)] = df.select("name", "age").as[(String, Int)]
    ds.show()
  }
}
