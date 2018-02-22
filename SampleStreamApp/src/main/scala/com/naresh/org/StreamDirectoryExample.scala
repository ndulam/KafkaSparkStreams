package com.naresh.org

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
/*
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.1 --jars ~/.ivy2/jars/kafka-clients-0.10.2.1.jar --class com.naresh.org.StreamDirectoryExample ./target/SampleStreamApp-1.0-SNAPSHOT.jar

bin/kafka-console-producer.sh -broker-list localhost:9092 --topic jeep
 */

object StreamDirectoryExample
{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Stream directory Example")

    val ssc = new StreamingContext(sparkConf,Seconds(2))
    val lines = ssc.textFileStream("/ndulam/streamdir")
     val words = lines.flatMap(_.split(" "))
    val wordcounts = words.map(x=>(x,1)).reduceByKey(_+_)
    wordcounts.print()

    ssc.start()

    ssc.awaitTermination()

  }

}