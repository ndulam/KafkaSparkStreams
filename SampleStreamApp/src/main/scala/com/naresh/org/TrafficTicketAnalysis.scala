package com.naresh.org
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import java.sql.Timestamp

import org.apache.spark
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.functions._


object TrafficTicketAnalysis
{

  case class trafficTickets(SummonsNumber:Long,PlateID:String,RegistrationState:String,PlateType:String,IssueDate:String,ViolationCode:Int,VehicleBodyType:String,VehicleMake:String,IssuingAgency:String,StreetCode1:Int,StreetCode2:Int,StreetCode3:Int,VehicleExpirationDate:String,ViolationLocation:Int,ViolationPrecinct:Int,IssuerPrecinct:Int,IssuerCode:Int,IssuerCommand:String,IssuerSquad:String,ViolationTime:String,TimeFirstObserved:String,ViolationCounty:String,ViolationInFrontOfOrOpposite:String,HouseNumber:String,StreetName:String,IntersectingStreet:String,DateFirstObserved:String,LawSection:String,SubDivision:String,ViolationLegalCode:String,DaysParkingInEffect:String,FromHoursInEffect:String,ToHoursInEffect:String,VehicleColor:String,UnregisteredVehicle:String,VehicleYear:String,MeterNumber:String,FeetFromCurb:String,ViolationPostCode:String,ViolationDescription:String,NoStandingorStoppingViolation:String,HydrantViolation:String,DoubleParkingViolation:String,Latitude:String,Longitude:String,CommunityBoard:String,CommunityCouncil:String,CensusTract:String,BIN:String,BBL:String,NTA:String)


  def main(args: Array[String]): Unit =
  {

    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()
    import spark.implicits._
    val file= spark.read.parquet("hdfs://localhost:9000/npt/parquet/part-00000-b0654542-adcc-4091-b25b-e0bf0ebb10d0-c000.snappy.parquet").as[trafficTickets]
    file.groupBy("ViolationCode").agg(count("SummonsNumber").alias("Totalcount")).orderBy(desc("Totalcount")).collect().foreach(println)

    file.groupBy(year(to_date(unix_timestamp($"IssueDate","mm/dd/yyyy").cast("timestamp")))).agg(count("SummonsNumber").alias("tc")).orderBy(desc("tc")).collect().foreach(println)

    file.groupBy("ViolationCode").pivot("RegistrationState").agg(count("SummonsNumber").alias("Total_count")).collect.foreach(println)
    file.groupBy("VehicleBodyType","VehicleMake").pivot("RegistrationState").agg(count("SummonsNumber").alias("Total_count")).take(5).foreach(println)

    //file.filter(col("VehicleBodyType").equals("TT") && col("VehicleMake").equals("INTER") && col("RegistrationState").equals("99")).count

    val head = spark.sparkContext.makeRDD(Seq("SummonsNumber,PlateID,RegistrationState,PlateType,IssueDate,ViolationCode,VehicleBodyType,VehicleMake,IssuingAgency,StreetCode1,StreetCode2,StreetCode3,VehicleExpirationDate,ViolationLocation,ViolationPrecinct,IssuerPrecinct,IssuerCode,IssuerCommand,IssuerSquad,ViolationTime,TimeFirstObserved,ViolationCounty,ViolationInFrontOfOrOpposite,HouseNumber,StreetName,IntersectingStreet,DateFirstObserved,LawSection,SubDivision,ViolationLegalCode,DaysParkingInEffect,FromHoursInEffect,ToHoursInEffect,VehicleColor,UnregisteredVehicle,VehicleYear,MeterNumber,FeetFromCurb,ViolationPostCode,ViolationDescription,NoStandingorStoppingViolation,HydrantViolation,DoubleParkingViolation,Latitude,Longitude,CommunityBoard,CommunityCouncil,CensusTract,BIN,BBL,NTA"))
    val headdf = head.toDF
    val d = file.rdd.map(x=>x.toString)
    val filedf = file.toDF




  }
}
