package com.naresh.org

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object multiTopicStructuredstream
{

  def main(args: Array[String]): Unit =
  {

    val userTopic = args(0)
    val sensorTopic = args(1)
    val resultTopic = args(2)
    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()

    val mySchema = StructType(Seq(StructField("name",StringType,true),StructField("city",StringType,true),StructField("state",StringType,true),
      StructField("county",StringType,true),StructField("browser",StringType,true),
      StructField("time",StringType,true),StructField("isActive",StringType,true),StructField("longitude",StringType,true),
      StructField("lattitude",StringType,true),StructField("zipcode",StringType,true)))

    val schema = new StructType().add("dcname", StringType).add("source",
      MapType(
        StringType,
        new StructType()
          .add("id", LongType)
          .add("ip", StringType)
          .add("description",StringType)
          .add("temp", LongType)
          .add("c02_level", LongType)
          .add("geo",
            new StructType()
              .add("lat", DoubleType)
              .add("longi", DoubleType))))


    import spark.implicits._
    val userdf: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", userTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()
    val sensordf: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", sensorTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()


    val sesnorSchemadf:DataFrame = sensordf.selectExpr("CAST(value AS STRING)").select(from_json($"value", schema) as "data")
    val explodedDF = sesnorSchemadf.select($"data.dcname",explode($"data.source"))

    val zipdf:DataFrame = userdf.selectExpr("CAST(value AS STRING)").select(from_json($"value", mySchema) as "data").select("data.zipcode").alias("zipcode")

    val allColumns = explodedDF.select(col("dcname"),col("key"),col("value.id"),col("value.ip"),
      col("value.description"),col("value.temp"),col("value.c02_level"),col("value.geo.lat"),col("value.geo.longi"))

    val sensorquery = allColumns.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()


    val userquery = zipdf.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    sensorquery.awaitTermination()
    userquery.awaitTermination()

  }



}

//spark-submit --packages org.apache.spark:spark-streaming_2.11:2.2.1 --jars $(echo ~/.ivy2/jars/*.jar | tr ' ' ',') --class com.naresh.org.multiTopicStructuredstream  target/SampleStreamApp-1.0-SNAPSHOT.jar useractivity sensor