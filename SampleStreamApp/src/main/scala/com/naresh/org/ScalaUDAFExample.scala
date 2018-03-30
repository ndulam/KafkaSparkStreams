package com.naresh.org

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object ScalaUDAFExample
{
  private class SumProductAggregation extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = new StructType().add("price",DoubleType).add("quantity",LongType)

    override def bufferSchema: StructType = new StructType().add("result",DoubleType)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = { buffer.update(0,0.0)  }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      val sum = buffer.getDouble(0)
      val price =input.getDouble(0)
      val qty = input.getLong(1)

      buffer.update(0,sum+(qty*price))

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getDouble(0)+buffer2.getDouble(0))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

  def main(args: Array[String]): Unit = {

    val conf       = new SparkConf().setAppName("Scala UDAF Example")
    val spark      = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val obj = new  SumProductAggregation
    spark.udf.register("SUMPRODUCT", obj)
    val inventorydf =  spark.read.json("hdfs://localhost:9000/yelp/inventory.json")
    inventorydf.createOrReplaceTempView("inventory")
    //inventorydf.selectExpr("CAST(Make AS STRING) AS Make","CAST(Model AS STRING) AS Model","CAST(RetailValue AS DOUBLE) AS RetailValue","CAST(Stock AS DOUBLE) AS Stock").createOrReplaceTempView("inventory")

    spark.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake FROM inventory GROUP BY Make").show()

  }

//spark-submit --class com.naresh.org.ScalaUDAFExample
}


