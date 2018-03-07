package com.naresh.org

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.types._

case class sensor(dcname:String,devicename:String,id:Int,ip:String,description:String,temp:Long,co2level:Long,lat:Double,longi:Double,eventTime:Timestamp)

object sensor
{
  def apply(dcname:String,devicename:String,id:Int,ip:String,description:String,temp:Long,co2level:Long,lat:Double,longi:Double,eventTime:Long): sensor =
    new sensor(dcname, devicename, id, ip, description, temp, co2level, lat, longi, new Timestamp(eventTime))
}
case class location(id:Int,zipCode:Int)

object StreamStaticJoin
{
    def main(args: Array[String]): Unit =
    {
      val sourceTopic = args(0)
      val endTopic = args(1)
      val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()

     import spark.implicits._
      var csv = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/KafkaApps/resources/mapfile.csv").as[location]

      while(true)
        {
          val query = startQuery(spark,args(0),csv)
          query.awaitTermination(10)
          query.stop()
          csv = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/KafkaApps/resources/mapfile.csv").as[location]
        }

      def startQuery(spark: SparkSession,topic: String,ds:Dataset[location]):StreamingQuery=
        {
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

          import spark.implicits._
          val df: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", sourceTopic).option("startingOffsets", "latest").option("failOnDataLoss","false").load()

          val zipdf:DataFrame = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", schema) as "data")
          val explodedDF = zipdf.select($"data.dcname",explode($"data.source"))

          val allColumns = explodedDF.select( col("dcname"),col("key"),col("value.id"),col("value.ip"),
          col("value.description"),col("value.temp"),col("value.c02_level"),col("value.geo.lat"),col("value.geo.longi"),col("value.eventTime"))

          val ds:Dataset[sensor] = allColumns.map(row=>sensor(row.getString(0),row.getString(1),row.getInt(2),row.getString(3),row.getString(4),row.getLong(5),row.getLong(6),row.getDouble(7),row.getDouble(8),row.getLong(9))).as[sensor]

          val result = ds.join(csv,Seq("id"))
          result.writeStream.outputMode("complete").format("console").option("truncate","false").start()
         // result.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
          //.option("topic", endTopic).option("checkpointLocation", "/tmp/sparkcheckpoint1/").queryName("kafka spark streaming kafka1").outputMode("update").start()

        }
    }

  }