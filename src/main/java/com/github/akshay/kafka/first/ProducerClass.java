package com.github.akshay.kafka.first;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerClass
{
    public static void main(String[] args)
    {
        String bootStrapServers = "localhost:9092";

        System.out.println("Hello World!!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("Java_Topic", "Hello World!");

        // Send Data  --> Async Sending of Data
        producer.send(record);

        // Flush Data
        producer.flush();

        // Flush and Close Producer Data
        producer.close();

    }
}
