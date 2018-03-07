package com.naresh.org


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object ModesExperiment
{

  def main(args: Array[String]) :Unit = {


    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()
    import spark.implicits._
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")


    val windowedCounts = words.withWatermark("timestamp", "1 minutes").groupBy(
      window($"timestamp", "10 seconds", "5 seconds"), $"word").count()
      //.orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()


  }

  }