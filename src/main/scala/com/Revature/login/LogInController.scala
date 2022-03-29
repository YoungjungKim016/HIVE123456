package com.Revature.login

import java.sql.{DriverManager, Statement}
import scala.io.StdIn.readLine

object LogInController {
  val sqlUrl = "jdbc:mysql://localhost:3306/projects"
  val driver = "com.mysql.cj.jdbc.Driver"
  val sqlUname = "root"
  val sqlPw = "asdqwe123"
  var conn = DriverManager.getConnection(sqlUrl, sqlUname, sqlPw)
  val stmt:Statement = conn.createStatement()

  def logIn(username: String, password: String): Boolean = {
    val rs1 = stmt.executeQuery(s"SELECT count(*) FROM users WHERE username='$username' AND password = '$password';")
    rs1.next()
    val userCount = rs1.getInt(1)
    if (userCount == 1)
      true
    else
      false

  }

  def updatePW(username: String, password: String): Unit = {
      val newPassword = readLine("New password: ")
      val sql1 = (s"UPDATE users SET password = '$newPassword' WHERE username ='$username' AND password = '$password';")
      stmt.executeUpdate(sql1)

  }

  def signUp(): Unit = {
    var username = readLine("Enter username: ")
    var password = readLine("Enter password: ")
    var rs1 = stmt.executeQuery(s"SELECT * FROM users HAVING username='$username';")
    while (rs1.next()) {
      println(
        """
          |username is already taken.
          |choose another username.
          |""".stripMargin)
      username = readLine("Enter username: ")
      password = readLine("Enter password: ")
      rs1 = stmt.executeQuery(s"SELECT * FROM users HAVING username='$username';")
    }

    val sql0 = s"INSERT INTO users (username, password, admin) " +
      s"VALUES ('$username', '$password', 0);"
    stmt.executeUpdate(sql0)
  }

  def adminLogin(username: String, password: String): Boolean = {
    val rs1 = stmt.executeQuery(s"SELECT count(*) FROM users WHERE username='$username' AND password = '$password';")
    rs1.next()
    val userCount = rs1.getInt(1)
    if (userCount == 1)
      true
    else
      false
  }

}