package com.Revature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object bpdemo324 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    //---- Session Starts Successfully
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    //--- Welcome Line
    println(
      """
        |Find your next favorite shows.
        |Enter your username to start.
        |""".stripMargin)

    spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("DROP TABLE IF EXISTS Student_Major")
    spark.sql(
      """create table Student_Major(
        |id INT, year STRING, fname STRING, lname STRING)
        |partitioned by (major STRING)
        |row format delimited fields terminated by ','
        |stored as textfile""".stripMargin)
    spark.sql("DROP table IF EXISTS StudentInfo")
    spark.sql(
      """create table StudentInfo(
        |id INT, major STRING, year STRING, fname STRING, lname STRING)
        |row format delimited fields terminated by ','
        |stored as textfile""".stripMargin)
    spark.sql("LOAD DATA LOCAL INPATH 'bpdemo.txt' INTO TABLE StudentInfo")
    spark.sql("insert overwrite table Student_Major partition(major) select id,year,fname,lname,major from StudentInfo")
    spark.sql("SELECT Count(*) AS TOTALCOUNT FROM StudentInfo").show()
    spark.sql("SHOW PARTITIONS Student_Major").show()
    spark.close()
  }
}