package com.naresh.org

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

object YelpAnalysis
{

  case class business(business_id:String,name:String,neighborhood:String,address:String,city:String,state:String,postal_code:String,latitude:String,longitude:String,stars:String,review_count:String,is_open:Double,categories:String)
  case class ba(business_id:String,AcceptsInsurance:String,ByAppointmentOnly:String,BusinessAcceptsCreditCards:String,BusinessParking_garage:String,BusinessParking_street:String,BusinessParking_validated:String,BusinessParking_lot:String,BusinessParking_valet:String,HairSpecializesIn_coloring:String,HairSpecializesIn_africanamerican:String,HairSpecializesIn_curly:String,HairSpecializesIn_perms:String,HairSpecializesIn_kids:String,HairSpecializesIn_extensions:String,HairSpecializesIn_asian:String,HairSpecializesIn_straightperms:String,RestaurantsPriceRange2:String,GoodForKids:String,WheelchairAccessible:String,BikeParking:String,Alcohol:String,HasTV:String,NoiseLevel:String,RestaurantsAttire:String,Music_dj:String,Music_background_music:String,Music_no_music:String,Music_karaoke:String,Music_live:String,Music_video:String,Music_jukebox:String,Ambience_romantic:String,Ambience_intimate:String,Ambience_classy:String,Ambience_hipster:String,Ambience_divey:String,Ambience_touristy:String,Ambience_trendy:String,Ambience_upscale:String,Ambience_casual:String,RestaurantsGoodForGroups:String,Caters:String,WiFi:String,RestaurantsReservations:String,RestaurantsTakeOut:String,HappyHour:String,GoodForDancing:String,RestaurantsTableService:String,OutdoorSeating:String,RestaurantsDelivery:String,BestNights_monday:String,BestNights_tuesday:String,BestNights_friday:String,BestNights_wednesday:String,BestNights_thursday:String,BestNights_sunday:String,BestNights_saturday:String,GoodForMeal_dessert:String,GoodForMeal_latenight:String,GoodForMeal_lunch:String,GoodForMeal_dinner:String,GoodForMeal_breakfast:String,GoodForMeal_brunch:String,CoatCheck:String,Smoking:String,DriveThru:String,DogsAllowed:String,BusinessAcceptsBitcoin:String,Open24Hours:String,BYOBCorkage:String,BYOB:String,Corkage:String,DietaryRestrictions_dairy_free:String,DietaryRestrictions_gluten_free:String,DietaryRestrictions_vegan:String,DietaryRestrictions_kosher:String,DietaryRestrictions_halal:String,DietaryRestrictions_soy_free:String,DietaryRestrictions_vegetarian:String,AgesAllowed:String,RestaurantsCounterService:String)
  case class bh(business_id:String,monday:String,tuesday:String,wednesday:String,thursday:String,friday:String,saturday:String,sunday:String)
  case class checkin(business_id:String,weekday:String,hour:String,checkins:Int)
  case class review(review_id:String,user_id:String,business_id:String,stars:String,date:String,text:String,useful:String,funny:String,cool:String)
  case class tip(text:String, date:String, likes:String, business_id:String, user_id:String)
  case class user(user_id:String, name:String, review_count:Int, yelping_since: java.sql.Timestamp, friends:String, useful:Int, funny:Int, cool:Int, fans:Int, elite:String, average_stars:Double,compliment_hot:Int, compliment_more:Int, compliment_profile:Int, compliment_cute:Int, compliment_list:Int, compliment_note:Int, compliment_plain:Int, compliment_cool:Int, compliment_funny:Int, compliment_writer:Int, compliment_photos:Int)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Kafka source stream App1").master("local[*]").getOrCreate()
    /*

    val businessdf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_business.csv")
    val businessAttributesdf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_business_attributes.csv")
    val businesshoursdf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_business_hours.csv")
    val checkindf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_checkin.csv")
    val reviewdf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_review.csv")
    val tipdf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_tip.csv")
    val userdf=spark.read.option("header", "true").option("inferSchema", "true").option("escape","\"").csv("hdfs://localhost:9000/yelp/yelp_user.csv")

    val businessds=businessdf.as[business]
    val temp=businessAttributesdf.withColumnRenamed("DietaryRestrictions_dairy-free","DietaryRestrictions_dairy_free").withColumnRenamed("DietaryRestrictions_gluten-free","DietaryRestrictions_gluten_free").withColumnRenamed("DietaryRestrictions_soy-free","DietaryRestrictions_soy_free")
    val businessAttrds=temp.as[ba]
    val bhourds=businesshoursdf.as[bh]
    val checkds=checkindf.as[checkin]
    val reviewds=reviewdf.as[review]
    val tipds=tipdf.as[tip]
    val userds=userdf.as[user]

    */

    import spark.implicits._
    val businessds = spark.read.parquet("hdfs://localhost:9000/yelp/businessds/part*").as[business]
    val businessAttrds = spark.read.parquet("hdfs://localhost:9000/yelp/businessattds/part*").as[ba]
    val bhoursds =  spark.read.parquet("hdfs://localhost:9000/yelp/bhourds/part*").as[bh]
    val checkds=spark.read.parquet("hdfs://localhost:9000/yelp/checkds/part*").as[checkin]
    val reviewds=spark.read.parquet("hdfs://localhost:9000/yelp/reviewds/part*").as[review]
    val tipds = spark.read.parquet("hdfs://localhost:9000/yelp/tipds/part*").as[tip]
    val userds = spark.read.parquet("hdfs://localhost:9000/yelp/userds/part*").as[user]

    val tc = broadcast(businessds)
    //Finding business without without business attributes
    businessds.join(broadcast(businessAttrds),Seq("business_id"),"leftanti")

    //enhance businessds with businessattrds
    val detailedbusiness = businessds.join(broadcast(businessAttrds),Seq("business_id")).join(broadcast(bhoursds),Seq("business_id"))


    //review enhanced with Business data
    val businessreviews = reviewds.join(broadcast(detailedbusiness),Seq("business_id"))


    // reviews enhanced with user data
    val userreviews=reviewds.join(userds,Seq("user_id"))

    //reviews with users with tips
    reviewds.join(broadcast(userds),Seq("user_id")).join(broadcast(tipds),Seq("user_id"))

    reviewds.withColumnRenamed("stars","reviewstars").join(broadcast(businessds),Seq("business_id")).groupBy("stars").agg(count("business_id"))
    //each Business tips
    tipds.select(col("text"),col("business_id")).groupBy(col("business_id")).agg(collect_list("text").alias("tips")).join(broadcast(businessds.select(col("name"),col("business_id"))),Seq("business_id")).write.parquet("hdfs://localhost:9000/yelp/tipbusiness2")

    //user activity
    //reviewds.select(col("text").alias("review_text"),col("user_id")).join(broadcast(userds.select(col("user_id"),col("name"))),Seq("user_id"))
    reviewds.select(col("user_id"),col("business_id"),col("text").alias("review_text")).join(broadcast(userds.select(col("user_id"),col("name").alias("user_name"),col("yelping_since"))),Seq("user_id")).join(broadcast(businessds.select(col("business_id"),col("name").alias("business_name"))),Seq("business_id")).select(col("user_name"),col("business_name"),col("yelping_since"),col("review_text")).write.parquet("hdfs://localhost:9000/yelp/useractivity")

    //state wise number of stared businesses
    val pivot=reviewds.select($"business_id",col("stars").alias("review_star") ,$"user_id").join(broadcast(businessds),Seq("business_id")).select($"name",$"neighborhood",$"address",$"city",$"state",$"postal_code",$"review_count",$"review_star").orderBy(desc("review_star")).groupBy("state").pivot("review_star").agg(count("name"))

    reviewds.withColumnRenamed("stars","review_star").join(broadcast(businessds.filter(row=>row.state=="SC")),Seq("business_id"))

    //what is the time around most checked in
    checkds.select("business_id","hour").groupBy("hour").agg(count("business_id").alias("TotalCheckins")).orderBy(desc("TotalCheckins")).collect.foreach(println)

    //For each business when what is the most time checkins happened
    //each business hour wise max, min checkins
    checkds.groupBy("business_id","hour").agg(count("business_id").alias("Total_Count")).groupBy("business_id","hour").agg(min("Total_Count"),max("Total_Count"))

    //each business hour wise total check ins
    checkds.filter(r=>r.checkins>0).groupBy("business_id","hour").pivot("weekday").agg(sum("checkins"))

    //each business day wise check ins
    checkds.filter(r=>r.checkins>0).groupBy("business_id").pivot("weekday").agg(sum("checkins"))

    //each business wise day wise hour wise total check ins
    checkds.groupBy("business_id","weekday","hour").agg(sum("checkins").alias("total_checkins"))


    //each business for each day each hour max and min check ins


    val temp=checkds.groupBy("business_id","hour").pivot("weekday").agg(collect_list("checkins")).map(row=>(row.getString(0),row.getString(1),row.getList(2).size))


    //Each business day wise total checkins
   val daywisecheckins = checkds.groupBy("business_id").pivot("weekday").agg(sum("checkins"))

    val consolreviews=reviewds.filter(row=>row.stars!=null).withColumn("stars",reviewds("stars").cast("integer")).groupBy("business_id").agg(count("stars").alias("count_reviwes"),sum("stars").alias("total_reviews"))
    val consolidatedbs=businessds.join(bhoursds,Seq("business_id"))

    val detailedbsnsdetails = consolreviews.join(broadcast(consolidatedbs),Seq("business_id"))
    val hourwisecheckinlistcoll=checkds.groupBy("business_id","weekday").pivot("hour").agg(collect_list("checkins"))

    def array_max(x: mutable.WrappedArray[Int]): Int = {
      if(x.length>0)
        x.max
      else
        100
    }
    val maxCheckins = udf(array_max _)

   val hourwisemaxcheckins = hourwisecheckinlistcoll.select($"business_id",$"weekday",maxCheckins($"0:00"),maxCheckins($"1:00"),maxCheckins($"2:00"),maxCheckins($"3:00"),maxCheckins($"4:00"),maxCheckins($"5:00"),maxCheckins($"6:00"),maxCheckins($"7:00"),maxCheckins($"8:00"),maxCheckins($"9:00"),maxCheckins($"10:00"),maxCheckins($"11:00"),maxCheckins($"12:00"),maxCheckins($"13:00"),maxCheckins($"14:00"),maxCheckins($"15:00"),maxCheckins($"16:00"),maxCheckins($"17:00"),maxCheckins($"18:00"),maxCheckins($"19:00"),maxCheckins($"20:00"),maxCheckins($"21:00"),maxCheckins($"22:00"),maxCheckins($"23:00"))


    val businessdaywisecheckins = checkds.groupBy("business_id").pivot("weekday").agg(collect_list("checkins").alias("checkins"))

    val businessdaywisemaxcheckins = businessdaywisecheckins.select($"business_id",maxCheckins($"Fri").alias("FriMaxCheckins"),maxCheckins($"Mon").alias("MonMaxCheckins"),maxCheckins($"Tue").alias("TueMaxCheckins"),maxCheckins($"Wed").alias("WedMaxCheckins"),maxCheckins($"Thu").alias("ThuMaxCheckins"),maxCheckins($"Sat").alias("SatMaxCheckins"),maxCheckins($"Sun").alias("SunMaxCheckins"))

    val businessdaywisetotalcheckins = checkds.groupBy("business_id").pivot("weekday").agg(sum("checkins"))


    val filterbusinessds=  businessds.select(col("stars"),col("review_count")).filter(row=>{
      val temp= row.getString(0)
      var ret = true
      try
      {
        var t=temp.toInt
      }
      catch
        {
          case e: Exception => ret=false
        }
      ret
    }
    )






  }


}


