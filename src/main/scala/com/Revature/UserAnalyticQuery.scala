package com.Revature

import com.Revature.HiveDataIns.execQuery
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object UserAnalyticQuery {
  def searchByAge(spark: SparkSession): Unit = {
    val typeAge = readLine(
      """
        |     Please choose content rating
        |     (G, PG, PG-13, R, TV-Y, TV-Y7, TV-G, TV-PG, TV-14, TV-MA)
        |
        |     Option: """.stripMargin)
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
      s"FROM netflixshow WHERE netflixshow.rating = '$typeAge'")
  }

  def searchByYear(spark: SparkSession): Unit = {
    print("     Please type the year released \n     from: ")
    val typeYear1 = readInt()
    print("     to: ")
    val typeYear2 = readInt()
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
      s"FROM netflixshow WHERE netflixshow.release_year BETWEEN '$typeYear1' AND '$typeYear2'")
  }

  def searchByGenre(spark: SparkSession): Unit = {
    val typeGenre = readLine(
      """
        |     Please choose genre
        |     (Action, Sci-Fi, Comedies, Horror, Children, Dramas, etc...)
        |
        |     Option: """.stripMargin)
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in " +
      s"FROM netflixshow WHERE netflixshow.listed_in LIKE '%$typeGenre%'")
  }

  def detailSearch(spark: SparkSession): Unit = {
    val detailGenre = readLine("     Choose genre: ")
    print("     Type year released \n     from: ")
    val detailYear1 = readInt()
    print("     to: ")
    val detailYear2 = readInt()
    val detailCountry = readLine("     Choose country: ")
    execQuery(spark,s"SELECT type, title, director, cast, country, release_year, rating, duration, listed_in FROM netflixshow " +
      s"WHERE netflixshow.listed_in LIKE '%$detailGenre%' AND " +
      s"netflixshow.release_year BETWEEN '$detailYear1' AND '$detailYear2' AND " +
      s"netflixshow.country LIKE '%$detailCountry%'")
  }

}
