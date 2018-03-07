package com.naresh.org

import org.apache.spark.sql.{Row, SparkSession}


object csvTest
{

  def queryYahoo(row: Row) : Int = { return 10; }

  def main(args: Array[String]): Unit =
  {

    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()
    val csv = spark.read.option("header", "true").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/KafkaApps/resources/us_postal_codes.csv")

    csv.repartition(5).rdd.foreachPartition{ p => p.foreach(r => { queryYahoo(r) })}

    println("************")
    println(csv.count())
    println("*********************")

    spark.stop()
  }


}