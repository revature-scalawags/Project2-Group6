package com.revatureData.group6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, explode, split}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** This object captures live, realtime tweet streams from a twitter API with Spark Streaming.
 * It then joins incoming batches with a dataset of negative words that are deployed to executors as
 * a broadcast variables. Spark SQL is used for analytics.  all negative words inside the
 * the text of all live tweets are sorted their negative words are summed up.
 * The driver is set to capture for a 1hour intervals.
 */
object TrendingNegativity {

  //data set case class.
  case class BadWords(id: Int, word: String)

  def main(args: Array[String]): Unit = {
    //Set logging lever and establish connection to the twitter streaming API.
    Logger.getLogger("org").setLevel(Level.WARN)
    val streamer = TweetStreamRunner()

    //Start spark streaming
    Future {
      streamer.streamToDirectory()
    }

    //Create spark DF for incoming stream.
    val spark = SparkSession.builder
      .appName("TweetsTrending")
      .master("local[*]")
      .getOrCreate()

    //Create spark DF for negative words.//Create spark DF for negative words.//Create spark DF for negative words.
    val badWordSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("word", StringType, nullable = true)

    import spark.implicits._
    val badWordDictionary = spark.read
      .option("sep", ",")
      .schema(badWordSchema)
      .csv("data/negative-words.csv")
      .as[BadWords]

    //Capture stream.
    val staticDF = spark.read.json("tweetstream.tmp")
    val streamDF = spark.readStream.schema(staticDF.schema).json("data/twitterstream")
    streamDF.printSchema()

    //Split tweet into a array of words.
    val wordsDF = streamDF.select(split(col("data.text"), "\\s+")
      .as("words_array"))
      .drop("data.text")
      .withColumn("words", explode($"words_array"))

    //Broadcast bad word DS and join to tweeted words DF.
    val badWordJoin = wordsDF.join(
      broadcast(badWordDictionary),
      wordsDF("words") <=> badWordDictionary("word")
    ).cache()
    badWordJoin.explain()

    //Group by and sort.
    val badWordCount = badWordJoin
      .groupBy("word")
      .count()
      .sort($"count".desc)

    //Results
    println("Capturing Twitter Stream...")
    badWordCount.writeStream.outputMode("complete").format("console").start().awaitTermination(3600000)

    spark.stop()
  }
}
