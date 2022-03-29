package com.Revature

import com.Revature.HiveDataIns.{execQuery, loadCsv}
import org.apache.spark.sql.SparkSession
import com.Revature.UserAnalyticQuery.{countPG13}

object HiveTester {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    loadCsv(spark, "/Users/ykmcair/Downloads/netflix_titles.csv")
    execQuery(spark,"SELECT * FROM netflixshow")
    countPG13(spark)


  }

}
