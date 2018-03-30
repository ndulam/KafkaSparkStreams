package com.naresh.org

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.types._

object SensorAnalysis
{

  case class sensor(dcname:String,devicename:String,id:Int,ip:String,description:String,temp:Long,co2level:Long,lat:Double,longi:Double,eventTime:java.sql.Timestamp)

  object sensor
  {
    def apply(carId:String,devicename:String,id:Int,ip:String,description:String,temp:Long,co2level:Long,lat:Double,longi:Double,eventTime:Long): sensor =
      new sensor(carId, devicename, id, ip, description, temp, co2level, lat, longi, new java.sql.Timestamp(eventTime))
  }
  case class location(id:Int,zipCode:Int)
  def main(args: Array[String]): Unit =
  {
    val sourceTopic = args(0)
    val endTopic = args(1)
    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()
    val schema = new StructType().add("dcname", StringType).add("source",
      MapType(
        StringType,
        new StructType()
          .add("id", IntegerType)
          .add("ip", StringType)
          .add("description",StringType)
          .add("temp", LongType)
          .add("c02_level", LongType)
          .add("geo",
            new StructType()
              .add("lat", DoubleType)
              .add("longi", DoubleType)).add("eventTime",LongType)))
    import org.apache.spark.sql.functions.unix_timestamp
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", sourceTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

    val zipdf:DataFrame = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", schema) as "data")
    val explodedDF = zipdf.select($"data.dcname",explode($"data.source"))

    val allColumns = explodedDF.select( col("dcname"),col("key") as "source",col("value.id"),col("value.ip"),
      col("value.description"),col("value.temp"),col("value.c02_level"),col("value.geo.lat"),col("value.geo.longi"),col("value.eventTime"))

    val ds:Dataset[sensor] = allColumns.map(row=>sensor(row.getString(0),row.getString(1),row.getInt(2),row.getString(3),row.getString(4),row.getLong(5),row.getLong(6),row.getDouble(7),row.getDouble(8),row.getLong(9))).as[sensor]


    val csv = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/KafkaApps/resources/mapfile.csv").as[location]

    val result = ds.join(csv,Seq("id"))

    csv.printSchema()
    ds.printSchema()
    result.printSchema()

    val stream1=result.writeStream.outputMode("update").format("console").option("truncate","false").start()

    stream1.awaitTermination()

    //val result = ds.withWatermark("eventTime", "5 seconds").groupBy(window($"eventTime","20 seconds","10 seconds"), $"devicename").agg(count("temp").alias("Total Temperature"))


    //result.writeStream.outputMode("update").format("console").option("truncate", "false").start()


    //allColumns.selectExpr("CAST(dcname AS STRING) AS KEY","CAST(ip AS STRING) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      //.option("topic", endTopic).option("checkpointLocation", "/tmp/sparkcheckpoint/").queryName("kafka spark streaming kafka").outputMode("update").start()


  }

}

//bin/kafka-console-consumer.sh --bootstrap-server localhost:9093,localhost:9094,localhost:9092 --topic jeep
//spark-submit --packages org.apache.spark:spark-streaming_2.11:2.2.1 --jars $(echo ~/.ivy2/jars/*.jar | tr ' ' ',') --class com.naresh.org.SensorAnalysis target/SampleStreamApp-1.0-SNAPSHOT.jar useractivity jeep
