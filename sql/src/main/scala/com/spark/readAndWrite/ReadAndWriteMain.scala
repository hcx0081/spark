package com.spark.readAndWrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadAndWriteMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadAndWriteMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
    val df: DataFrame = spark.read.csv()
    df.show()
    
    df.write.save()
  }
}
