package com.revatureData.group6


import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}

import org.apache.http.HttpEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet


case class TweetStreamRunner(entity: HttpEntity) {

  def streamToDirectory() {
    if (entity != null) {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var (lineNumber, line) = (1, reader.readLine())
      val (linesPerFile, milliseconds) = (1000, System.currentTimeMillis())
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"twitterstream/tweetstream-$milliseconds-${lineNumber/linesPerFile}")
          )
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }
}

object TweetStreamRunner {
  def apply(): TweetStreamRunner = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    val httpGet = new HttpGet(uriBuilder.build)
    val bearerToken = System.getenv("BEARER_TOKEN")
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    new TweetStreamRunner(entity)
  }
}