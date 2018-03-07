package com.naresh.org

import org.apache.spark.sql._


object FilteringFile
{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()

    val file=spark.sparkContext.textFile("/Users/nd2629/Desktop/test.txt")

    val indexedRDD = file.zipWithIndex().map{case(x,y)=>
    {
      var t = x.split(",")
      var a:Int = 0;
      for(temp <- t)
      {
        if(temp.length()!=0)
          a = a+1
      }
      (x,a,y)
    }}

    indexedRDD.persist()
    import spark.sqlContext.implicits._
    val goodRecords = indexedRDD.filter{case(x,y,z)=>
    {
      var result = true
      if(y<4)
        result = false

      result
    }
    }.map{case(x,y,z) => (x)}.map(x=>Record(x)).toDF

    val rejectRecords = indexedRDD.filter{case(x,y,z)=>
    {
      var result = false
      if(y<4)
        result = true

      result
    }
    }.map{case(x,y,z) => (x)}.map(x=>Record(x)).toDF


    rejectRecords.write.mode(SaveMode.Append).parquet("/Users/nd2629/Desktop/reject")

  }

  case class Record(line:String)

}