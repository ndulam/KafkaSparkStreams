package com.naresh.org

import org.apache.spark.sql.{Row, SparkSession}


object csvTest
{

  case class trafficTickets(Summons_Number:Long,Plate_ID:String,Registration_State:String,Plate_Type:String,Issue_Date:String,Violation_Code:Int,Vehicle_Body_Type:String,Vehicle_Make:String,Issuing_Agency:String,Street_Code1:Int,Street_Code2:Int,Street_Code3:Int,Vehicle_Expiration_Date:Int,Violation_Location:Int,Violation_Precinct:Int,Issuer_Precinct:Int,Issuer_Code:Int,Issuer_Command:String,Issuer_Squad:String,Violation_Time:String,Time_First_Observed:String,Violation_County:String,Violation_In_Front_Of_Or_Opposite:String,House_Number:String,Street_Name:String,Intersecting_Street:String,Date_First_Observed:String,Law_Section:String,Sub_Division:String,Violation_Legal_Code:String,Days_Parking_In_Effect:String,From_Hours_In_Effect:String,To_Hours_In_Effect:String,Vehicle_Color:String,Unregistered_Vehicle:String,Vehicle_Year:String,Meter_Number:String,Feet_From_Curb:String,Violation_Post_Code:String,Violation_Description:String,No_Standing_or_Stopping_Violation:String,Hydrant_Violation:String,Double_Parking_Violation:String,Latitude:String,Longitude:String,Community_Board:String,Community_Council:String,Census_Tract:String,BIN:String,BBL:String,NTA:String)

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