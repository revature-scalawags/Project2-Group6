package com.revatureData.group6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.functions.{col, concat, explode, regexp_replace, split}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.annotation.tailrec

import scala.collection.mutable.MutableList
import scala.io.Source

object FollowerRecommender {

  case class UsersSet(id: String, screenName: String, followersCount: Int = 0, friendsCount: Int = 0)
  case class FriendsSet(id: String, screenName: String, friends: String)

  @tailrec
    def concatenateTailrec(aString: Seq[String], n: Int, accumulator: String): String =
    if (n < 9 || n >= aString.length) accumulator
    else concatenateTailrec(aString, n-1, accumulator + aString(n).concat("|"))

  def getUserDS(df: DataFrame, spark: SparkSession): Dataset[UsersSet] = {
    import spark.implicits._
    val colsToRemove = Seq("lang", "lastSeen", "tweetId", "tags", "friends")
    val filteredDF = df.drop(colsToRemove:_*)

    filteredDF.as[UsersSet]
  }

  def formatFriendsDS: MutableList[FriendsSet] = {
    val (ab, pattern) = (MutableList[FriendsSet](), """/[\[\]"]+/g""".r)
    val lines = Source.fromFile("data/user-data.csv")

    for (line <- lines.getLines()) {
      val fields = line.split(',').map(_.trim)
      if (fields.length > 1) {

        fields.drop(1)
        val friendList = concatenateTailrec(fields, fields.length -1, "")
        pattern.replaceAllIn(friendList, "")

        val fs = FriendsSet(fields(0), fields(1), friendList)
        ab.+=(fs)
        }
    }
    ab
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length != 1) {
      println("You must pass in one argument parameter as a single Twitter username.")
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

    val friendList = formatFriendsDS
    val friendsDS = friendList.toDS()
      friendsDS.printSchema()
    val userDS = getUserDS(userDF, spark)
    userDS.printSchema()

    val friendTable = friendsDS.select("id", "screenName", "friends")
    friendTable.show(20)

//    val withFriends = friendsDS
//      .select(split(col("value.friends"), "|")
//      .as("friendsArr"))
//      .drop("friends")
//      withFriends.show()
//      .withColumn("friendsArr", split(col("scrubbedFriends"), ","))
//      .drop("scrubbedFriends")
//    reformattedFriendsDS.printSchema()

//    usersWithFriends.printSchema()
//    val user = reformattedDS.filter($"screenName" === args(0))
//    val usrFollowingList = user
//      .withColumn("usrFollowing", explode($"friendsArr"))
//      .drop("friendsArr")
//    val allUsrFollowingList = reformattedDS
//      .filter($"id" =!=  "user.id")
//      .withColumn("usrFollowing", explode($"friendsArr"))
//      .drop("friendsArr")
//    user.show()
//    usrFollowingList.printSchema()
  }
}
