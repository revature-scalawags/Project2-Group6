package com.revatureData.group6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, explode, split}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TrendingNegativity {

  case class BadWords(id: Int, word: String)

  def main(args: Array[String]): Unit = {
    val streamer = TweetStreamRunner()

    Future {
      streamer.streamToDirectory()
    }

    val spark = SparkSession.builder()
      .appName("TweetsTrending")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val badWordSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("word", StringType, nullable = true)

    import spark.implicits._
    val badWordDictionary = spark.read
      .option("sep", ",")
      .schema(badWordSchema)
      .csv("data/negative-words.csv")
      .as[BadWords]

    val staticDF = spark.read.json("tweetstream.tmp")
    val streamDF = spark.readStream.schema(staticDF.schema).json("data/twitterstream")
    streamDF.printSchema()

    val wordsDF = streamDF.select(split(col("data.text"), "\\s+")
      .as("words_array"))
      .drop("data.text")
      .withColumn("words", explode($"words_array"))

    val badWordJoin = wordsDF.join(
      broadcast(badWordDictionary),
      wordsDF("words") <=> badWordDictionary("word")
    )
    badWordJoin.explain()

    val badWordCount = badWordJoin
      .groupBy("word")
      .count()
      .sort($"count".desc)

    println("Capturing Twitter Stream...")
    badWordCount.writeStream.outputMode("complete").format("console").start().awaitTermination(3600000)

    badWordCount.printSchema()
    spark.stop()
  }
}
