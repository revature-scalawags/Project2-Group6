package com.revatureData.group6

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.functions.{col, explode, regexp_replace, split}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FollowerRecommender {

  case class UsersInfo(id: Long, screenName: String, followersCount: Int = 0, friendsCount: Int = 0, friends: String)

  def getProperDataset(df: DataFrame, spark: SparkSession): Dataset[UsersInfo] = {
    import spark.implicits._
    val colsToRemove = Seq("lang", "lastSeen", "tweetId", "tags")
    val filteredDF = df.drop(colsToRemove:_*)

    filteredDF.as[UsersInfo]
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("You must pass in one argument as a single Twitter username.")
      System.exit(-1)
    }

    val spark = SparkSession
      .builder
      .appName("FollowerRecommender")
      .master("local[*]")
      .getOrCreate()

    val userSchema = new StructType()
      .add("id", LongType, nullable = false)
      .add("screenName", StringType, nullable = true)
      .add("tags", StringType, nullable = true)
      .add("followersCount", IntegerType, nullable = true)
      .add("friendsCount", IntegerType, nullable = true)
      .add("lang", StringType, nullable = true)
      .add("lastSeen", DoubleType, nullable = true)
      .add("tweetId", DoubleType, nullable = true)
      .add("friends", StringType, nullable = true)

    import spark.implicits._
    val userDF = spark.read
      .option("header", "true")
      .schema(userSchema)
      .csv("data/user-data.csv")

    val userDS = getProperDataset(userDF, spark)
    userDS.printSchema()

    val reformattedDS = userDS
      .withColumn("scrubbedFriends", regexp_replace('friends,"/[\\[\\]\"]+/g",""))
      .drop("friends")
      .withColumn("friendsArr", split(col("scrubbedFriends"), ","))
      .drop("scrubbedFriends")
    val user = reformattedDS.filter($"screenName" === args(0))
    val usrFollowingList = user.withColumn("following", explode($"friendsArr"))
    usrFollowingList.printSchema()
  }
}
