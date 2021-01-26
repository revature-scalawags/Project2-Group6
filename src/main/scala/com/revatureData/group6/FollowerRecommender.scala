package com.revatureData.group6

import org.apache.log4j.{Level, Logger}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.{col, explode, regexp_replace, split}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.annotation.tailrec

object FollowerRecommender extends Serializable with Logging {

  case class UsersSet(id: String, screenName: String, followersCount: Int = 0, friendsCount: Int = 0)
  case class FriendsSet(id: String, friendString: String)

  @tailrec
  def concatenateTailrec(aString: Seq[String], n: Int, accumulator: String): String =
    if (n < 0 || n >= aString.length) accumulator
    else concatenateTailrec(aString, n - 1, accumulator + aString(n) + '|')

  def prepareFriendsDF(row: String): (String, String) = {
    val fields = row.split(",").map(_.trim)
    val friendsArray = for {
      (x, i) <- fields.zipWithIndex
      if i >= 8
    } yield x
    val friendsList = concatenateTailrec(friendsArray, friendsArray.length - 1, "")

    (fields(0), friendsList)
  }

  def getUserDS(df: DataFrame, spark: SparkSession): Dataset[UsersSet] = {
    import spark.implicits._
    val colsToRemove = Seq("lang", "lastSeen", "tweetId", "tags", "friends")
    val filteredDF = df.drop(colsToRemove: _*)

    filteredDF.as[UsersSet]
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)




    if (args.length != 1) {
      println("You must pass in one argument parameter as a single Twitter username.")
      logger.warn("no argument provided.")
      System.exit(-1)
    }

    val spark = SparkSession
      .builder
      .appName("FollowerRecommender")
      .master("local[*]")
      .getOrCreate()

    val userSchema = new StructType()
      .add("id", StringType, nullable = false)
      .add("screenName", StringType, nullable = true)
      .add("tags", StringType, nullable = true)
      .add("followersCount", IntegerType, nullable = true)
      .add("friendsCount", IntegerType, nullable = true)
      .add("lang", StringType, nullable = true)
      .add("lastSeen", LongType, nullable = true)
      .add("tweetId", StringType, nullable = true)
      .add("friends", StringType, nullable = true)


    import spark.implicits._
    val userDF = spark.read
      .schema(userSchema)
      .csv("data/user-data.csv")


    val friendsRDD = spark.read
      .option("header", "true")
      .textFile("data/user-data.csv").rdd

    val header = friendsRDD.first()
    val rdd = friendsRDD.filter(row => row != header)
    val friendsDF = rdd.map(prepareFriendsDF).toDF("id", "friendString")

    val userDS = getUserDS(userDF, spark)
    val friendsDS = friendsDF.as[FriendsSet]

    val scrubbedFriendsDF = friendsDS
      .withColumn("cleanedFriends", regexp_replace(friendsDS("friendString"), "[\\[\\]\"\\s+]", ""))
      .drop("friendString")

    val withFriendsArr = scrubbedFriendsDF.select($"id", split(col("cleanedFriends"), "\\|")
      .as("friendsArr"))
      .drop("cleanedFriends")

    val userFriendJoin = userDS.join(withFriendsArr, "id")

    val twitterUser = userFriendJoin.filter($"screenName" === args(0))
    val everyoneElse = userFriendJoin.filter($"screenName" =!= args(0))

    if (twitterUser.take(1).isEmpty){
      println(s"The Twitter screen name ${args(0)} was not found.\n Check spelling. This user may not exist in this dataset.")
      logger.error("screen name not found.")
      System.exit(-1)
    }

    val withAllUserFriends = twitterUser.select("screenName", "friendsArr", "friendsCount")
      .withColumn("friend", explode($"friendsArr"))
      .drop("friendsArr")
    val withAllOtherFriends = everyoneElse.select("id", "friendsArr")
      .withColumn("friends", explode($"friendsArr"))
      .drop("friendsArr")

    withAllUserFriends.join(
      withAllOtherFriends,
      withAllUserFriends("friend") <=> withAllOtherFriends("friends")
    )
      .groupBy("id")
      .count()
      .sort($"count".desc)
      .show(20)

    spark.stop()
  }
}
