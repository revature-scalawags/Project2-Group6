package com.revatureData.group6

import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TrendingNegativity {

  def loadDictionary: Map[Int, String] = {
    var badWords: Map[Int, String] = Map()
    val lines = Source.fromFile("data/negative_words.csv")

    for (line <- lines.getLines()) {
      val fields = line.split(',')
      if (fields.length > 1) {
        badWords += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    badWords
  }

  def main(args: Array[String]): Unit = {
    val streamer = TweetStreamRunner()

    Future {
      streamer.streamToDirectory()
    }
    val spark = SparkSession.builder()
      .appName("TweetsTrending")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val staticDF = spark.read.json("tweetstream.tmp")
    val streamDF = spark.readStream.schema(staticDF.schema).json("data/twitterstream")
    streamDF.printSchema()
  }
}
