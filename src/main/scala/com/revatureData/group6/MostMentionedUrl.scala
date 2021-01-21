package com.revatureData.group6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object MostNotedUrl {
    
    case class Tweet(value: String)
    

    def main(args: Array[String]){
        
        //i need to be able to get a list of file names at
        

        val spark = SparkSession
            .builder
            .appName(name="MostNotedLink")
            .getOrCreate()

        import spark.implicits._

        val input = spark.read.text(path = "s3://group6project2/Datalake/twitterstream/combined.txt").as[Tweet]

        val urls = input
            .select(explode(split(str=$"value", pattern="http*://*{1,}\\s")).alias(alias="url"))
            .filter(condition=$"url" =!= "")

        val lowercaseUrls = urls.select(lower($"url").alias(alias="url"))

        val urlCounts = lowercaseUrls.groupBy(col1="url").count()

        val urlCountsSorted = urlCounts.sort(sortCol = "count")

        urlCountsSorted.show(urlCountsSorted.count.toInt)

    }   
}