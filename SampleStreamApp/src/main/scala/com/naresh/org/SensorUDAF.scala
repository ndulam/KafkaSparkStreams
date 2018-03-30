package com.naresh.org

import com.naresh.org.ScalaUDAFExample.SumProductAggregation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SensorUDAF
{

  private class SensorUDAFAgg extends UserDefinedAggregateFunction
  {
    override def inputSchema: StructType = new StructType().add("source",
    MapType(
      StringType,
      new StructType()
        .add("id", IntegerType)
        .add("ip", StringType)
        .add("description", StringType)
        .add("temp", LongType)
        .add("c02_level", LongType)
        .add("geo",
          new StructType()
            .add("lat", DoubleType)
            .add("longi", DoubleType)).add("eventTime", LongType)))

    override def bufferSchema: StructType = new StructType().add("result",DoubleType)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = { buffer.update(0,0.0)  }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      val instruct = input.getStruct(0)

     // buffer.update(0,sum+(lat+longi))

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {buffer1.update(0,buffer1.getDouble(0)+buffer2.getDouble(0))}

    override def evaluate(buffer: Row): Any = {buffer.getDouble(0)}
  }

  def main(args: Array[String]): Unit = {

    val conf       = new SparkConf().setAppName("Scala UDAF Example")
    val spark      = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
   val schema= new StructType().add("value",new StructType().add("id",StringType).add("ip",StringType).add("description",StringType).add("temp",LongType).add("c02_level",LongType)
      .add("geo",new StructType().add("lat",DoubleType).add("longi",DoubleType)).add("eventTime",LongType))
    val obj = new  SensorUDAFAgg
    spark.udf.register("SUMPRODUCT", obj)
    val sensordf =  spark.read.json("hdfs://localhost:9000/yelp/sensor.json")
    import spark.implicits._
    //val temp = sensordf.selectExpr("CAST(value AS STRING)").select(from_json($"value", schema) as "data")
    //val explodedDF = sensordf.select($"dcname",explode($"source.sensor-igauge"))
    sensordf.createOrReplaceTempView("sensor")
    spark.sql("SELECT SUMPRODUCT(value) from sensor group by dcname").take(2)

  }

}


