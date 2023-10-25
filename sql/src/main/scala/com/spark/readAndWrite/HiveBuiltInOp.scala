package com.spark.readAndWrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveBuiltInOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HiveBuiltInOp")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    
    val df: DataFrame = spark.createDataFrame(Seq(("zs", 20), ("ls", 20), ("ww", 20)))
    df.createOrReplaceTempView("teacher")
    
    spark.sql("create table student(name string, age int)")
    
    spark.sql("show tables").show()
    // +---------+---------+-----------+
    // |namespace|tableName|isTemporary|
    // +---------+---------+-----------+
    // |  default|  student|      false|
    // |         |  teacher|       true|
    // +---------+---------+-----------+
    
    spark.stop()
  }
}
