package com.revatureData.group6

import scala.io.Source

object TrendingNegativity {

  def loadDictionary: Map[Int, String] = {
    var badWords: Map[Int, String] = Map()
    val lines = Source.fromFile("data/negative_words.csv")

    for (line <- lines.getLines()) {
      val fields = line.split(',')
      if (fields.length > 1) {
        badWords += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    badWords
  }

  def main(args: Array[String]): Unit = {

  }
}
