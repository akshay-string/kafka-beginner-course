package com.github.akshay.kafka.first;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerClassGroups
{
    public static void main(String[] args)
    {
        System.out.println("Hello from Consumer World!!");

        String bootStrapServer = "localhost:9092";
        String groupId = "my-third-app";
        String offsetConfig = "earliest";
        String consumerTopic = "Java_Topic";
        long pollDuration = 100;

        Logger logger = LoggerFactory.getLogger(ConsumerClassGroups.class.getName());

        // Creating Consumer Config by setting it's properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to a topic
        //consumer.subscribe(Collections.singleton(consumerTopic));       // For single topic
        consumer.subscribe(Arrays.asList(consumerTopic));                 // For multiple topic

        // Poll New Data from producer
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollDuration));

            for (ConsumerRecord<String, String> record : records)
            {
                logger.info("Key: " + record.key());
                logger.info("Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("Offset: " + record.offset());
            }
        }
    }
}
