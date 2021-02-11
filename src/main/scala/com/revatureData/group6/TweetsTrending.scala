package com.revatureData.group6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** This object captures live, realtime tweet streams from a twitter API with Spark Streaming.
 * It searches the text of each tweet for hashtags and finds to current top 20 trending hashtags worldwide.
 * Spark SQL is used to for analytics.
 * the text of all live tweets are sorted and their hastags are summed up.
 * The driver is set to capture for a 1hour intervals.
 */
object TweetsTrending {

  def main(args: Array[String]) {
    //Set logging lever and establish connection to the twitter streaming API.
    Logger.getLogger("org").setLevel(Level.ERROR)
    val streamer = TweetStreamRunner()

    //Start spark streaming
    Future {
      streamer.streamToDirectory()
    }
    //Create spark DF for incoming stream.
    val spark = SparkSession.builder()
      .appName("TweetsTrending")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //Capture stream.
    val staticDF = spark.read.json("tweetstream.tmp")
    val streamDF = spark.readStream.schema(staticDF.schema).json("data/twitterstream")
    streamDF.printSchema()


    //Split tweet into a array of words.
    val words = streamDF.select(split(col("data.text"), "\\s+")
      .as("words_array"))
      .drop("data.text")
      .withColumn("hashtag", explode($"words_array"))
    //filter for hashtags only. Organize the results.
    val hashtags = words.filter(words("hashtag").startsWith("#"))
      .groupBy("hashtag")
      .count()
      .sort($"count".desc)

    //Results
    println("Capturing Twitter Stream...")
    hashtags.writeStream.outputMode("complete").format("console").start().awaitTermination(60000)

    spark.stop()
  }
}
