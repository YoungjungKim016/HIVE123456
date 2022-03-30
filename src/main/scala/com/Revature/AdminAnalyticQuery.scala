package com.Revature

import com.Revature.HiveDataIns.execQuery
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object AdminAnalyticQuery {
  def numAllList(spark: SparkSession): Unit = {
    spark.sql("SELECT count(*) FROM datasetShow").show()
  }

  def movieVsTv(spark: SparkSession): Unit = {
    spark.sql("SELECT type, COUNT(*) AS count FROM datasetShow " +
      "GROUP BY type ORDER BY count DESC").show()
  }

  //def numDirByYear(spark: SparkSession): Unit = {
  //    val directorName = readLine("Type name of director: ")
  //    print("type year released \nfrom: ")
  //    val detailYear1 = readInt()
  //    print("to: ")
  //    val detailYear2 = readInt()
  //    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
  //      s"WHERE netflixshow.director LIKE '%$directorName%' AND " +
  //      s"netflixshow.release_year BETWEEN '$detailYear1' AND '$detailYear2'")
  //    execQuery(spark,s"SELECT COUNT(title) FROM netflixshow " +
  //      s"WHERE netflixshow.director LIKE '%$directorName%' AND " +
  //      s"netflixshow.release_year BETWEEN '$detailYear1' AND '$detailYear2'")
  //  }

  def numCountry(spark: SparkSession): Unit = {
    spark.sql("SELECT country, COUNT(*) AS count FROM datasetShow " +
      "GROUP BY country ORDER BY count DESC LIMIT 30").show(30)
  }

//  def numGenByYear(spark: SparkSession): Unit = {
//    val genByYear = readLine("Type genre: ")
//    print("type year released \nfrom: ")
//    val detailYear1 = readInt()
//    print("to: ")
//    val detailYear2 = readInt()
//    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
//      s"WHERE netflixshow.listed_in LIKE '%$genByYear%' AND " +
//      s"netflixshow.release_year BETWEEN '$detailYear1' AND '$detailYear2'")
//  }

  def numDirector(spark: SparkSession): Unit = {
    spark.sql("SELECT director, COUNT(*) AS count FROM datasetShow " +
      "GROUP BY director ORDER BY count DESC LIMIT 30").show(30)
  }

  //def numCastByYear(spark: SparkSession): Unit = {
  //    val castByYear = readLine("Type name of actor/actress: ")
  //    print("type year released \nfrom: ")
  //    val detailYear1 = readInt()
  //    print("to: ")
  //    val detailYear2 = readInt()
  //    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
  //      s"WHERE netflixshow.cast LIKE '%$castByYear%' AND " +
  //      s"netflixshow.release_year BETWEEN '$detailYear1' AND '$detailYear2'")

  def showDirector(spark: SparkSession): Unit = {
    val directorName = readLine("Type name of director: ")
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
      s"WHERE netflixshow.director LIKE '%$directorName%'")
  }

  def numCountryByYear(spark: SparkSession): Unit = {
    val countryName = readLine("Type name of country: ")
    execQuery(spark,s"SELECT country, release_year, COUNT(*) AS count FROM datasetShow " +
      s"WHERE country LIKE '%$countryName%' GROUP BY country, release_year ORDER BY release_year DESC")
  }
}

