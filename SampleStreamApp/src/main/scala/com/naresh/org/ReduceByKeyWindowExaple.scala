package com.naresh.org

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._



object ReduceByKeyWindowExaple
{
  def main(args: Array[String]): Unit =
  {


    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val kafkParams = Map[String,Object]("bootstrap.servers"->"localhost:9092,localhost:9093","key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "use_a_separate_group_id_for_each_stream")

    val topic = Set[String]("jeep").toSet

   // val lines = KafkaUtils.createStream()
   // val lines = KafkaUtils.createStream()


  }

}
