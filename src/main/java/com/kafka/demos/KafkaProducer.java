package com.kafka.demos;

import java.util.Properties;  
import java.util.concurrent.TimeUnit;  
  
import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import kafka.serializer.StringEncoder;  

public class KafkaProducer extends Thread {
	
	private String topic;

	public KafkaProducer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		
		Producer producer=createProducer();
		 int i=0;
		 while(true){
			 producer.send(new KeyedMessage<Integer,String>(topic,"message:"+i++)); 
			 try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		 }
	}

	private Producer createProducer() {

		Properties properties=new Properties();
		//声明zk
		properties.put("zookeeper.connect","192.168.0.85:9092,192.168.0.87:9092,192.168.0.88:9092");
		
		properties.put("Serializer",StringEncoder.class.getName());
		//声明kafka的broker
         properties.put("metadata.broker.list","192.168.0.85:9092,192.168.0.87:9092,192.168.0.88:9092");		
	
         return new Producer<Integer,String>(new ProducerConfig(properties));
	}
	
	    public static void main(String[] args) {
	    	//使用kafka集群中创建好的主题test
	    	  new KafkaProducer("test").start();
		}
}
