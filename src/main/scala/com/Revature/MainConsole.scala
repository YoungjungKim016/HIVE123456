package com.Revature

import com.Revature.AdminAnalyticQuery.allList
import com.Revature.HiveDataIns.{execQuery, loadCsv, loadCsvAdmin}
import com.Revature.login.LogInController.{adminLogin, logIn, signUp, updatePW}
import org.apache.spark.sql.SparkSession
import com.Revature.UserAnalyticQuery.{countPG13, detailSearch, searchByAge, searchByGenre, searchByYear}

import scala.io.StdIn.{readInt, readLine}

object MainConsole {
  def main(args: Array[String]): Unit = {
    println(
      """
        |
        |Welcome to YK Bank.
        |Please log in to start.
        |""".stripMargin)
    //===================================================================================
    var signMenu: Int = 0
    while (signMenu != 3) {
      println(
        """
          |What would you like to do today?
          |1: Sign In     2: Sign Up
          |3: Exit
          |""".stripMargin);

      print("Option: ")
      signMenu = readInt();
      signMenu match {
        case 1 =>
          val username = readLine("Enter username: ")
          val password = readLine("Enter password: ")
          if (logIn(username = s"$username", password = s"$password")) {
            println("hi")
          }
          else {
            println("cannot log in")
            //System.exit(1)
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
                |What would you like to do today?
                |1: pg13            2: search by age rating
                |3: search by year  4: search by genre
                |5: search detail   8: update password
                |9: Sign out
                |""".stripMargin);

            print("Option: ")
            menu = readInt();
            menu match {
              case 1 => countPG13(spark)
              case 2 => searchByAge(spark)
              case 3 => searchByYear(spark)
              case 4 => searchByGenre(spark)
              case 5 => detailSearch(spark)
              case 8 => println(
                """
                  |Please enter new password.
                  |""".stripMargin)
                updatePW(username: String, password: String)
              case 9 => println(
                """
                  |See you later
                  |""".stripMargin);

            }
          }

        case 2 => println(
          """
            |Please choose your username and password.
            |""".stripMargin);
          signUp()
          println(
            """account made successfully.
              |Welcome.""".stripMargin)

        case 123123098 => println(
          """
            |Admin access
            |""".stripMargin)
          val username = readLine("Admin:  ")
          val password = readLine("Password: ")
          if (adminLogin(username = s"$username", password = s"$password")) {
            println("success")
          }
          else {
            println("fail")
            //System.exit(1)
          }
          val spark: SparkSession = SparkSession
            .builder
            .appName("hello hive")
            .config("spark.master", "local")
            .enableHiveSupport()
            .getOrCreate()
          spark.sparkContext.setLogLevel("ERROR")
          loadCsvAdmin(spark, "netflix_titles.csv")
          var adminMenu: Int = 0
          while (adminMenu != 9) {
            println(
              """
                |What would you like to do today?
                |1: all list            2: search by age rating
                |3: search by year  4: search by genre
                |5: search detail   8: update password
                |9: Sign out
                |""".stripMargin);

            print("Option: ")
            adminMenu = readInt();
            adminMenu match {
              case 1 => allList(spark)
              case 2 => searchByAge(spark)
              case 3 => searchByYear(spark)
              case 4 => searchByGenre(spark)
              case 5 => detailSearch(spark)

              case 3 => println(
                """
                  |Good Bye
                  |""".stripMargin);
            }
          }



        // ========================================================================

      }

    }
  }
}
