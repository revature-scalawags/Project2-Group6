package com.revatureData.group6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split, udf}

import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TrendingNegativity {

  def loadDictionary: Map[String, Int] = {
    var badWords: Map[String, Int] = Map()
    val lines = Source.fromFile("data/negative-words.csv")

    for (line <- lines.getLines()) {
      val fields = line.split(',').map(_.trim)
      if (fields.length > 1) {
        badWords += (fields(1) -> 1)
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
    val dictionary = spark.sparkContext.broadcast(loadDictionary)

    val staticDF = spark.read.json("tweetstream.tmp")
    val streamDF = spark.readStream.schema(staticDF.schema).json("data/twitterstream")
    streamDF.printSchema()

    val words = streamDF.select("data.text")
//      .as("words_array"))
//      .drop("data.text")
//      .withColumn("words", explode($"words_array"))

    val lookUpWord : String => Int = (tweet: String) => tweet.split(" ").map(word => dictionary.value(word)).sum
    val lookUpWordUDF = udf(lookUpWord)

    val badWordCount = lookUpWordUDF(col("data.text"))
    println(badWordCount)
    val withCount = streamDF.withColumn("neg_word_count", badWordCount)
      val query = withCount.groupBy("neg_word_count")
      .count()
      .sort($"count".desc)

    println("Capturing Twitter Stream...")
    query.writeStream.outputMode("complete").format("console").start().awaitTermination(60000)

    query.printSchema()
    dictionary.destroy()
    spark.close()
  }
}
