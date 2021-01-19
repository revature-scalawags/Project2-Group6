package com.revatureData.group6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object TweetsTrending {

  def main(args: Array[String]) {
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
    val streamDF = spark.readStream.schema(staticDF.schema).json("twitterstream")
    streamDF.printSchema()


    val words = streamDF.select(split(col("data.text"), "\\s+")
      .as("words_array"))
      .drop("data.text")
      .withColumn("hashtag", explode($"words_array"))
    val hashtags = words.filter(words("hashtag").startsWith("#"))
      .groupBy("hashtag")
      .count()
      .sort($"count".desc)

    println("Capturing Twitter Stream...")
    hashtags.writeStream.outputMode("complete").format("console").start().awaitTermination(60000)

    spark.stop()
  }
}
