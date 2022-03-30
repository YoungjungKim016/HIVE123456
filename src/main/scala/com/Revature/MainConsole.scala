package com.Revature

import com.Revature.AdminAnalyticQuery.{numAllList, movieVsTv, numCountry, numDirector, showDirector, numCountryByYear}
import com.Revature.HiveDataIns.{execQuery, loadCsv, loadCsvAdmin}
import com.Revature.login.LogInController.{adminLogin, logIn, signUp, updatePW}
import org.apache.spark.sql.SparkSession
import com.Revature.UserAnalyticQuery.{detailSearch, searchByAge, searchByGenre, searchByYear}

import scala.io.StdIn.{readInt, readLine}

object MainConsole {
  def main(args: Array[String]): Unit = {
    println(
      """
        |     Find your next favorite shows.
        |
        |""".stripMargin)
    //===================================================================================
    var signMenu: Int = 0
    while (signMenu != 3) {
      println(
        """     What would you like to do today?
          |     1: Sign In    2: Sign Up    3: Exit
          |""".stripMargin);

      print("     Option: ")
      signMenu = readInt();
      signMenu match {
        case 1 =>
          val username = readLine("\n     Enter username: ")
          val password = readLine("     Enter password: ")
          if (logIn(username = s"$username", password = s"$password")) {
            println(
              """
                |     Please wait for loading.
                |""".stripMargin)
          }
          else {
            println(
              """
                |     Account not found.
                |     Please check your username or password.
                |""".stripMargin)
            System.exit(1)
          }
          val spark: SparkSession = SparkSession
            .builder
            .appName("hello hive")
            .config("spark.master", "local")
            .enableHiveSupport()
            .getOrCreate()
          spark.sparkContext.setLogLevel("ERROR")
          loadCsv(spark, "netflix_titles.csv")

          var menu: Int = 0
          while (menu != 9) {
            println(
              """
                |
                |     Welcome!
                |
                |     What would you like to do today?
                |     1: search by content rating     2: search by year
                |     3: search by genre              4: search detail
                |     8: update password              9: Sign out
                |""".stripMargin);

            print("     Option: ")
            menu = readInt();
            menu match {
              case 1 => searchByAge(spark)
              case 2 => searchByYear(spark)
              case 3 => searchByGenre(spark)
              case 4 => detailSearch(spark)
              case 8 => println(
                """
                  |     Please enter new password.
                  |""".stripMargin)
                updatePW(username: String, password: String)
              case 9 => println(
                """
                  |     See you again
                  |""".stripMargin);

            }
          }

        case 2 => println(
          """
            |     Please choose your username and password.
            |""".stripMargin);
          signUp()
          println(
            """
              |     New account was created successfully.
              |     Welcome.""".stripMargin)

        case 123123098 => println(
          """
            |     Admin access
            |""".stripMargin)
          val username = readLine("     Admin: ")
          val password = readLine("     Password: ")
          if (adminLogin(username = s"$username", password = s"$password")) {
            println("\n     Please wait...")
          }
          else {
            println("\n     Fail")
            System.exit(1)
          }
          val spark: SparkSession = SparkSession
            .builder
            .appName("hello hive")
            .config("spark.master", "local")
            .enableHiveSupport()
            .getOrCreate()
          spark.sparkContext.setLogLevel("ERROR")
          loadCsv(spark, "netflix_titles.csv")
          var adminMenu: Int = 0
          while (adminMenu != 9) {
            println(
              """
                |     Choose your option
                |
                |     1: Number of all shows   2: Movie vs TV
                |     3: Top Countries         4: Top Directors
                |     5: Shows by Director     6: Country by year
                |     8: Update password       9: Exit Admin Mode
                |""".stripMargin);

            print("     Option: ")
            adminMenu = readInt();
            adminMenu match {
              case 1 => numAllList(spark)
              case 2 => movieVsTv(spark)
              case 3 => numCountry(spark)
              case 4 => numDirector(spark)
              case 5 => showDirector(spark)
              case 6 => numCountryByYear(spark)
              case 8 => println(
                """
                  |     Please enter new password.
                  |""".stripMargin)
                updatePW(username: String, password: String)
              case 9 => println(
                """
                  |     Good Bye
                  |""".stripMargin);
            }
          }

        case 3 => println(
          """
            |     See you again.
            |""".stripMargin);

        // ========================================================================

      }

    }
  }
}
