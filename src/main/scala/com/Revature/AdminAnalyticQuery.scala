package com.Revature

import com.Revature.HiveDataIns.execQuery
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.io.StdIn._

object AdminAnalyticQuery {
  def numAllList(spark: SparkSession): Unit = {

    spark.sql("SELECT count(*) FROM datasetShow").show()

  }

  def movieVsTv(spark: SparkSession): Unit = {
    spark.sql("SELECT type, COUNT(*) AS count FROM datasetShow " +
      "GROUP BY type ORDER BY count DESC").show()
  }



  def numCountry(spark: SparkSession): Unit = {
    spark.sql("SELECT country, COUNT(*) AS count FROM datasetShow " +
      "GROUP BY country ORDER BY count DESC LIMIT 30").show(30)
  }



  def numDirector(spark: SparkSession): Unit = {
    spark.sql("SELECT director, COUNT(*) AS count FROM datasetShow " +
      "GROUP BY director ORDER BY count DESC LIMIT 30").show(30)
  }



  def showDirector(spark: SparkSession): Unit = {
    val directorName = readLine("     Type name of director: ")
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
      s"WHERE netflixshow.director LIKE '%$directorName%'")
  }

  def numCountryByYear(spark: SparkSession): Unit = {
    val countryName = readLine("     Type name of country: ")
    execQuery(spark,s"SELECT country, release_year, COUNT(*) AS count FROM datasetShow " +
      s"WHERE country LIKE '%$countryName%' GROUP BY country, release_year ORDER BY release_year DESC")
  }
}

