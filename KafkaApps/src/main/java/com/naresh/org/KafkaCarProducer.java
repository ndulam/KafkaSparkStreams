package com.naresh.org;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaCarProducer {

	public static void main(String[] args) throws InterruptedException 
	{
		String topic = args[0];
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
		//If the request fails, the producer can automatically retry,
		props.put("retries", 0);
			      
		//Specify buffer size in config
		props.put("batch.size", 16384);
			      
		//Reduce the no of requests less than 0   
		props.put("linger.ms", 1);
		//The buffer.memory controls the total amount of memory available to the producer for buffering.   
		props.put("buffer.memory", 33554432);
		String value;
		String carName;
		int speed;
		float acc;
		Random rand = new Random(); 
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int interval = 300;
		while(true)
			{
			value = "car"+rand.nextInt(10)+","+rand.nextInt(150)+","+rand.nextFloat() * 100+","+System.currentTimeMillis();
			producer.send(new ProducerRecord<String, String>(topic,"key" ,value));
			Thread.sleep(interval);
			}        
			     // producer.close();
}
}
