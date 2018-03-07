package com.naresh.org

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object DiffUpdateStateByKeyAndMapWithState {



  def main(args: Array[String]): Unit = {



    val sparkConf = new SparkConf().setAppName("First Stream App")

    val ssc = new StreamingContext(sparkConf,Seconds(2))
    ssc.checkpoint(".")
    val initialRDD = ssc.sparkContext.parallelize(List(("hello",1),("world",1)))


    val kafkParams = Map[String,Object]("bootstrap.servers"->"localhost:9092,localhost:9093","key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topic = Set[String]("test1","test2","test3").toSet

    val messages = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic,kafkParams))

    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordsDstream = words.map(x=>(x,1))

    val mappingFunc = (word:String,one:Option[Int],state:State[Int]) => {

      val sum = one.getOrElse(0)+state.getOption().getOrElse(0)
      val output = (word,sum)
      state.update(sum)
      println(s"MapWithState: key=$word value=$one state=$state")
      output
    }

    val updateFunc = (newValues: Seq[Int], oldValue: Option[Int]) =>{
      println(s"new values = ${newValues.mkString(",")} oldValue=$oldValue")
      Some(newValues.foldLeft(oldValue.getOrElse(0))(_ + _))
    }


    val updatestateByKeyDstream = wordsDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))

    val mapWithStateDStream = wordsDstream.updateStateByKey(updateFunc)

    updatestateByKeyDstream.foreachRDD(rdd => {
      rdd.foreach(pair => println(s"k=${pair._1} v=${pair._2}"))
    })

    mapWithStateDStream.foreachRDD(rdd => {
      rdd.foreach(pair => println(s"k=${pair._1} v=${pair._2}"))
    })

   // stateDstream.print()
    ssc.start()
    ssc.awaitTermination()

  }



}