package com.naresh.org

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SensorAnalysis
{

  def main(args: Array[String]): Unit =
  {
    val sourceTopic = args(0)
    val endTopic = args(1)
    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()
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
    val df: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", sourceTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

    val zipdf:DataFrame = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", schema) as "data")
    val explodedDF = zipdf.select($"data.dcname",explode($"data.source"))
    val allColumns = explodedDF.select(col("dcname"),col("key"),col("value.id"),col("value.ip"),
      col("value.description"),col("value.temp"),col("value.c02_level"),col("value.geo.lat"),col("value.geo.longi"))

    allColumns.selectExpr("CAST(dcname AS STRING) AS KEY","CAST(ip AS STRING) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", endTopic).option("checkpointLocation", "/tmp/sparkcheckpoint/").queryName("kafka spark streaming kafka").outputMode("update").start()
    spark.streams.awaitAnyTermination();

  }
}

//bin/kafka-console-consumer.sh --bootstrap-server localhost:9093,localhost:9094,localhost:9092 --topic jeep
//spark-submit --packages org.apache.spark:spark-streaming_2.11:2.2.1 --jars $(echo ~/.ivy2/jars/*.jar | tr ' ' ',') --class com.naresh.org.SensorAnalysis target/SampleStreamApp-1.0-SNAPSHOT.jar useractivity jeep
