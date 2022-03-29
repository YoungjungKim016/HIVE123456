package com.Revature

import com.Revature.HiveDataIns.execQuery
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object AdminAnalyticQuery {
  def allList(spark: SparkSession): Unit = {
    spark.sql("SELECT * FROM datasetShow").show()
  }
}

//def searchByAge(spark: SparkSession): Unit = {
//    val typeAge = readLine(
//      """
//        |please choose content rating
//        |(G, PG, PG-13, R, TV-Y, TV-Y7, TV-G, TV-PG, TV-14, TV-MA)
//        |
//        |option: """.stripMargin)
//    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
//      s"FROM netflixshow WHERE netflixshow.rating = '$typeAge'")
//  }