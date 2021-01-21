package com.revatureData.group6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer



object MostNotedUrl {
    
    case class Tweet(data: Data)
    case class Data(id: String,text: String)
    

    def main(args: Array[String]): Unit={
        
        
        

        val spark = SparkSession
            .builder
            .appName(name="MostNotedUrl")
            .master(master="local[*]")
            .getOrCreate()

        import spark.implicits._

        val input = spark.read.json(path = "s3://group6project2/Datalake/twitterstream/combined.txt").as[Tweet]

        val urls = input
            .select(explode(split(str=$"data.text", pattern="\\s")).alias(alias="url"))
            .filter(condition=$"url" =!= "")
        val urlDF = urls.toDF()
        
        
        val links = urlDF.filter($"url" rlike "(http)" )
        val linkCounts = links.groupBy(col1="url").count()
        val linkCountsSorted = linkCounts.sort("count")
        linkCountsSorted.show(linkCountsSorted.count.toInt)
        

    }   
}