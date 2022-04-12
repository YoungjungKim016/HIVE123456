name := "Project1"

version := "0.2"

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1" % "provided"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.28"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.11"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % "test"

libraryDependencies += "com.lihaoyi" %% "upickle" % "1.5.0"
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.5.0"