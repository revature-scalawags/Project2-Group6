name := "Project2-Group6"

version := "1.0"
organization := "com.revatureData.group6"
scalaVersion := "2.12.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.1",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime,
  "org.apache.httpcomponents" % "httpclient" % "4.5.12"
)