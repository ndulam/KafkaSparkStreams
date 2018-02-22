package com.naresh.org

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

object BlockListWords
{
  @volatile private var instance:Broadcast[Seq[String]] = null

  def getInstance(sc:SparkContext): Broadcast[Seq[String]] = {

    if(instance == null) {
      synchronized{
        if(instance == null)
          {
            var wordblacklist = Seq("hi","hello")
            instance = sc.broadcast(wordblacklist)
          }
      }
    }
    instance
  }
}

object DroppedWordsCounter
{
  @volatile private var instance:LongAccumulator = null

  def getInstance(sc:SparkContext) : LongAccumulator = {
    if(instance==null)
      {
        synchronized{
          if(instance == null)
            {
              instance = sc.longAccumulator("wordsinblacklistcounter")
            }
        }
      }
  instance
  }
}


object WordCountRecoverable
{

  def createContext(): StreamingContext =
  {
    println("creating new context")
    val outputfile = new File("~/out")
    if(outputfile.exists()) outputfile.delete()
    val sparkConf = new SparkConf().setAppName("Recoverablewordcount");
    val ssc = new StreamingContext(sparkConf,Seconds(1))
    ssc.checkpoint("~/checkpoint/")
    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val wordcounts = words.map((_,1)).reduceByKey(_+_)
    wordcounts.foreachRDD{ (rdd: RDD[(String, Int)], time: Time) =>
      val blackList = BlockListWords.getInstance(rdd.sparkContext)
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)


      val counts = rdd.filter{case(word,count) =>
          if(blackList.value.contains(word)){
            droppedWordsCounter.add(count)
            false
          } else
            {
              true
            }
      }.collect().mkString("[", ", ", "]")
      val output = "Counts at time"+ time +" "+counts
      println(output)
      println("Dropped "+droppedWordsCounter.value +" word(s) totally ")
      println("Appending to "+ outputfile.getAbsolutePath)
      Files.append(output+ "\n", outputfile, Charset.defaultCharset())
    }
  ssc
  }

  def main(args: Array[String]): Unit =
  {
    val ssc = StreamingContext.getOrCreate("~/checkpoint/",()=>createContext())

    ssc.start()
    ssc.awaitTermination()

  }

}

