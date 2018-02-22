package com.naresh.org


import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._

/*
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.1 --jars ~/.ivy2/jars/kafka-clients-0.10.2.1.jar --class com.naresh.org.MapWithState ./target/SampleStreamApp-1.0-SNAPSHOT.jar

bin/kafka-console-producer.sh -broker-list localhost:9092 --topic jeep
 */

object MapWithState
{


  def main(args: Array[String]): Unit = {



    val sparkConf = new SparkConf().setAppName("First Stream App")

    val ssc = new StreamingContext(sparkConf,Seconds(2))
    ssc.checkpoint(".")
    val initialRDD = ssc.sparkContext.parallelize(List(("hello",1),("world",1)))


    val kafkParams = Map[String,Object]("bootstrap.servers"->"localhost:9092,localhost:9093","key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topic = Set[String]("jeep").toSet

    val messages = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic,kafkParams))

    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordsDstream = words.map(x=>(x,1))

    val mappingFunc = (word:String,one:Option[Int],state:State[Int]) => {

      val sum = one.getOrElse(0)+state.getOption().getOrElse(0)
      val output = (word,sum)
      state.update(sum)
      output
    }
    val stateDstream = wordsDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()

  }



}