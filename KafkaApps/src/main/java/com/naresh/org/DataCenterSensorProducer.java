package com.naresh.org;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.json.JsonSerializer;

public class DataCenterSensorProducer
{
	public static void main(String[] args) throws IOException
	{
		
		//String[] sources = {"sensor-igauge","sensor-ipad","sensor-inest","sensor-istick"};
		String[] descriptions = {"Sensor attached to the container ceilings","Sensor ipad attached to carbon cylinders",
				"Sensor attached to the factory ceilings","Sensor embedded in exhaust pipes in the ceilings"};
		String[] data = {"sensor-igauge:dc-101:1:68.28.91.21:Sensor attached to the container ceilings","sensor-igauge:dc-101:2:67.185.72.2:Sensor ipad attached to carbon cylinders",
				"sensor-igauge:dc-101:3:208.109.163.213:Sensor attached to the factory ceilings","sensor-ipad:dc-101:4:204.116.105.64:Sensor embedded in exhaust pipes in the ceilings",
				"sensor-ipad:dc-102:5:68.28.91.25:Sensor embedded in exhaust pipes in the ceilings",
				"sensor-inest:dc-102:6:67.185.72.6:Sensor attached to the container ceilings","sensor-inest:dc-102:7:208.109.163.217:Sensor embedded in exhaust pipes in the ceilings",
				"sensor-inest:dc-102:8:204.116.105.68:Sensor ipad attached to carbon cylinders","sensor-istick:dc-102:9:208.109.163.219:ensor attached to the container ceilings","sensor-istick:dc-102:10:204.116.105.10:Sensor ipad attached to carbon cylinders"};
		Random rand = new Random(); 
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Sensor> source = new HashMap<String, Sensor>();
		Sensor sensor; 
		Geo geo;
		DCSensor dsc;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
		props.put("acks", "all");
		//If the request fails, the producer can automatically retry,
		props.put("retries", 0);
			      
		props.put("batch.size", 16384);
			      
		//Reduce the no of requests less than 0   
		props.put("linger.ms", 1);
		//The buffer.memory controls the total amount of memory available to the producer for buffering.   
		props.put("buffer.memory", 33554432);
		Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
		JsonNode  jsonNode;
		while(true)
		{
		int count = rand.nextInt(5)+1;
		String[] token=null;
		for(int i=0;i<count; i++)
		{
		token = data[rand.nextInt(data.length)].split(":");
		geo = new Geo((double)rand.nextInt(100), (double)rand.nextInt(100));
		//Sensor(int id, String ip, String description, int temp, int c02_level, Geo geo)
		sensor = new Sensor(Integer.parseInt(token[2]), token[3], token[4], rand.nextInt(100), rand.nextInt(2000), geo);
		source.put(token[0], sensor);
		}
		dsc = new DCSensor(token[1],source);
		jsonNode = objectMapper.valueToTree(dsc);
		System.out.println(jsonNode);
		ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>("useractivity",jsonNode);
		producer.send(rec);
		}
		
	}
}
