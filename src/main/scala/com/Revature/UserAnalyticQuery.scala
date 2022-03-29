package com.Revature

import com.Revature.HiveDataIns.execQuery
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object UserAnalyticQuery {
  def countPG13(spark: SparkSession): Unit = {
    execQuery(spark,"SELECT count(*) FROM netflixshow WHERE netflixshow.rating = 'PG-13'")
  }

  def searchByAge(spark: SparkSession): Unit = {
    val typeAge = readLine(
      """
        |please choose content rating
        |(G, PG, PG-13, R, TV-Y, TV-Y7, TV-G, TV-PG, TV-14, TV-MA)
        |
        |option: """.stripMargin)
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
      s"FROM netflixshow WHERE netflixshow.rating = '$typeAge'")
  }

  def searchByYear(spark: SparkSession): Unit = {
    print("please type the year released \nfrom: ")
    val typeYear1 = readInt()
    print("to: ")
    val typeYear2 = readInt()
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
      s"FROM netflixshow WHERE netflixshow.release_year BETWEEN '$typeYear1' AND '$typeYear2'")
  }

  def searchByGenre(spark: SparkSession): Unit = {
    val typeGenre = readLine(
      """
        |please choose genre
        |(Action, Sci-Fi, Comedies, Horror, Children, Dramas, etc...)
        |
        |option: """.stripMargin)
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
      s"FROM netflixshow WHERE netflixshow.listed_in LIKE '%$typeGenre%'")
  }

  def detailSearch(spark: SparkSession): Unit = {
    val detailGenre = readLine("choose genre: ")
    print("type year released \nfrom: ")
    val detailYear1 = readInt()
    print("to: ")
    val detailYear2 = readInt()
    val detailCountry = readLine("choose country: ")
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
      s"WHERE netflixshow.listed_in LIKE '%$detailGenre%' AND " +
      s"netflixshow.release_year BETWEEN '$detailYear1' AND '$detailYear2' AND " +
      s"netflixshow.country LIKE '%$detailCountry%'")
  }

}
