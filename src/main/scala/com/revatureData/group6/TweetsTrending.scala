package com.revatureData.group6

import org.apache.spark.sql.SparkSession
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object TweetsTrending {
  def main(args: Array[String]) {
    val streamer = new TweetStreamRunner

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

    val textQuery = streamDF.select($"data.text").writeStream.outputMode("append").format("console").start()
    println("Loading Twitter Stream...")
    textQuery.awaitTermination(60000)
  }
}
