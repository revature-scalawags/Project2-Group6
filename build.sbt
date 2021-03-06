name := "Project2-Group6"

version := "1.0"
organization := "com.revatureData.group6"
scalaVersion := "2.12.13"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime,
  "org.apache.httpcomponents" % "httpclient" % "4.5.12",
  "com.lihaoyi" %% "requests" % "0.6.5",
)
