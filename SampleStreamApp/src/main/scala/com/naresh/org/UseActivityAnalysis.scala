package com.naresh.org

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, _}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql._

object UseActivityAnalysis
{

 // case class UserActivity()


  def main(args: Array[String]): Unit =
  {
    val sourceTopic = args(0)
    val endTopic = args(1)
    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()


//{"name":"Mathew","city":"Johnston","state":"Illinois","county":"Clallam","browser":"IE","time":"1519185218257",
// "isActive":"Y","longitude":"35.6408","lattitude":"-88.0772","zipcode":"78208"}
  val mySchema = StructType(Seq(StructField("name",StringType,true),StructField("city",StringType,true),StructField("state",StringType,true),
  StructField("county",StringType,true),StructField("browser",StringType,true),
  StructField("time",StringType,true),StructField("isActive",StringType,true),StructField("longitude",StringType,true),
  StructField("lattitude",StringType,true),StructField("zipcode",StringType,true)))

    import spark.implicits._
    val df: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", sourceTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

    val df1: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", sourceTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

    val zipdf:DataFrame = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", mySchema) as "data").select("data.zipcode").alias("zipcode")

    val zipdf1:DataFrame = df1.selectExpr("CAST(value AS STRING)").select(from_json($"value", mySchema) as "data").select("data.zipcode").alias("zipcode")

    zipdf.selectExpr("CAST(zipcode AS STRING) AS KEY","CAST(zipcode AS STRING) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", endTopic).option("checkpointLocation", "/tmp/sparkcheckpoint/").queryName("kafka spark streaming kafka").outputMode("update").start()
    zipdf1.selectExpr("CAST(zipcode AS STRING) AS KEY","CAST(zipcode AS STRING) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", endTopic).option("checkpointLocation", "/tmp/sparkcheckpoint1/").queryName("kafka spark streaming kafka1").outputMode("update").start()
    spark.streams.awaitAnyTermination();

  }

}

//bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic jeep
//spark-submit --packages org.apache.spark:spark-streaming_2.11:2.2.1 --jars $(echo ~/.ivy2/jars/*.jar | tr ' ' ',') --class com.naresh.org.UseActivityAnalysis target/SampleStreamApp-1.0-SNAPSHOT.jar useractivity result
