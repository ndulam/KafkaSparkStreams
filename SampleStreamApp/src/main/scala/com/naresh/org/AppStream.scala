package com.naresh.org


import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


object AppStream {

  case class CarEvent(carId:String,speed:Option[Int],acceleration:Option[Double],timestamp:Timestamp)
  object CarEvent{
    def apply(rawString:String):CarEvent = {
      println("*******"+rawString+"**************")
      val parts = rawString.split(',')
      CarEvent(parts(0), Some(Integer.parseInt(parts(1))), Some(java.lang.Double.parseDouble(parts(2))), new Timestamp(parts(3).toLong))
    }
  }


  def main(args: Array[String]) :Unit = {

    val topic = args(0)
    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    import spark.implicits._

    val df: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", topic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

    df.printSchema()

    val cars:Dataset[CarEvent] = df.selectExpr("CAST(value AS STRING)").map(r=>CarEvent(r.getString(0)))

    val aggregates = cars.withWatermark("timestamp", "3 seconds").groupBy(window($"eventTime","4 seconds"), $"carId").agg(avg("speed").alias("speed"))

    aggregates.printSchema()

    val writeToKafka = aggregates.selectExpr("CAST(carId AS STRING) AS KEY","CAST(speed AS STRING) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", "fastcars").option("checkpointLocation", "/tmp/sparkcheckpoint/").queryName("kafka spark streaming kafka").outputMode("update").start()

    spark.streams.awaitAnyTermination();

  }

}
