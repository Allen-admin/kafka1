package com.kafka.demos;

import java.util.HashMap;  
import java.util.List;  
import java.util.Map;  
import java.util.Properties;  
  
import kafka.consumer.Consumer;  
import kafka.consumer.ConsumerConfig;  
import kafka.consumer.ConsumerIterator;  
import kafka.consumer.KafkaStream;  
import kafka.javaapi.consumer.ConsumerConnector;  
 
/** 
 * 接收数据 
 * 接收到: message: 10 
        接收到: message: 11 
        接收到: message: 12 
       接收到: message: 13 
       接收到: message: 14 
 * @author zm 
 * 
 */  

public class KafkaConsumer extends Thread {

	private String topic;

	public KafkaConsumer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();  
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();  
        // 一次从主题中获取一个数据  
        topicCountMap.put(topic, 1);
         Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);  
          // 获取每次接收到的这个数据  
         KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
         ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();  
         while(iterator.hasNext()){ 
             String message = new String(iterator.next().message());  
             System.out.println("接收到: " + message); 
	}
	      }
	
	
         private ConsumerConnector createConsumer() {  
             Properties properties = new Properties();  
               //声明zk 
             properties.put("zookeeper.connect", "192.168.0.85:9092,192.168.0.87:9092,192.168.0.88:9092"); 
         
             // 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
             properties.put("group.id", "group1");  
             return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));  
          }  
           
         public static void main(String[] args) {  
        	// 使用kafka集群中创建好的主题 test
        	 new KafkaConsumer("test").start();     
         }  
}
