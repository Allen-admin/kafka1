package com.mogujie.jkafkaconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by liyun on 17/3/16.
 */

public class KafkaConsumerTest {
   
	public static void main(String[] args) {
        //consume("search.dump.xitemfix1");
        producer("search.dump.xitemfix1");
    }

	public static void consume(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.85:9092,192.168.0.87:9092,192.168.0.88:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 1000);
      
         //earliest(消费早期的数据)  latest(消费最近的数据)
        props.put("auto.offset.reset", "earliest");  
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<TopicPartition> topicPartitionList = new ArrayList<TopicPartition>();
        TopicPartition topicPartition = new TopicPartition(topicName, 3); //因为默认都是一个分区,所以都是0分区
        topicPartitionList.add(topicPartition);
        consumer.subscribe(Arrays.asList(topicName));
        while (true) {
            Long lastOffset = consumer.endOffsets(topicPartitionList).get(topicPartition);
            Long beginOffset = consumer.beginningOffsets(topicPartitionList).get(topicPartition);
            System.out.println(lastOffset + ":" + beginOffset);
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.println(record.timestamp());
                System.out.println(record.offset());
            }
        }
    }

    public static void producer(String topic) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.0.85:9092,192.168.0.87:9092,192.168.0.88:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("batch.size", 16384);
        kafkaProperties.put("linger.ms", 0);
        kafkaProperties.put("buffer.memory", 33554432);
        Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);
        int i = 0;
        while (i < 10000) {
            String message = "你好，kafka";
            kafkaProducer.send(new ProducerRecord<String, String>(topic, message, message));
            i++;
            System.out.println(message);
        }
        kafkaProducer.flush();
    }
}