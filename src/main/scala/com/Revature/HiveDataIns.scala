package com.Revature

import org.apache.spark.sql.SparkSession

object HiveDataIns {
  def execQuery(spark: SparkSession, query: String): Unit = {
    spark.sql(query).show()
  }

  def loadCsv(spark: SparkSession, filePath: String): Unit = {
    val tableName = "netflixshow"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName(show_id STRING, type STRING, title STRING, director STRING, cast STRING, country STRING, date_added DATE, release_year INT, rating STRING, duration STRING, listed_in STRING, description STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS textfile")
    spark.sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE $tableName")

  }

  def loadCsvAdmin(spark: SparkSession, filePath: String): Unit = {
    val adminTable = "datasetShow"
    val adminTablePT = "datasetPT"
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(s"DROP TABLE IF EXISTS $adminTablePT")
    spark.sql(s"CREATE TABLE $adminTablePT(show_id STRING, title STRING, director STRING, cast STRING, country STRING, date_added DATE, release_year INT, rating STRING, duration STRING, listed_in STRING, description STRING) PARTITIONED BY (type STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS textfile")

    spark.sql(s"DROP TABLE IF EXISTS $adminTable")
    spark.sql(s"CREATE TABLE $adminTable(show_id STRING, type STRING, title STRING, director STRING, cast STRING, country STRING, date_added DATE, release_year INT, rating STRING, duration STRING, listed_in STRING, description STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS textfile")
    spark.sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE $adminTable")

    spark.sql(s"INSERT OVERWRITE TABLE $adminTablePT PARTITION(type) SELECT show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description FROM $adminTable")
    spark.sql(s"SHOW PARTITION $adminTablePT").show()

//    spark.sql(s"DROP TABLE IF EXISTS $adminTablePT")
//    spark.sql(s"CREATE TABLE $adminTablePT(show_id STRING, title STRING, director STRING, cast STRING, country STRING, date_added DATE, release_year INT, duration STRING, listed_in STRING, description STRING) PARTITIONED BY (type STRING, rating STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS textfile")
//
//    spark.sql(s"DROP TABLE IF EXISTS $adminTable")
//    spark.sql(s"CREATE TABLE IF NOT EXISTS $adminTable(show_id STRING, type STRING, title STRING, director STRING, cast STRING, country STRING, date_added DATE, release_year INT, rating STRING, duration STRING, listed_in STRING, description STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS textfile")
//    spark.sql(s"LOAD DATA LOCAL INPATH '$filePath' OVERWRITE INTO TABLE $adminTable")
//
//    spark.sql(s"INSERT OVERWRITE TABLE $adminTablePT PARTITION(type, rating) select show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description FROM $adminTable")
  }
}
