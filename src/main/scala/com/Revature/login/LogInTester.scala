package com.Revature.login

import com.Revature.login.LogInController.logIn

object LogInTester {
  def main(args: Array[String]): Unit = {


    if(logIn(username = "Admin", password = "password"))
      println("hi")
    else
      println("cannot log in")
  }

}
