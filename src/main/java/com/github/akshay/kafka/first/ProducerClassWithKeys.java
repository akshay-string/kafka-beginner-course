package com.github.akshay.kafka.first;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerClassWithKeys
{
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerClassWithKeys.class);
        String bootStrapServers = "localhost:9092";

        System.out.println("Hello World!!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 1; i <= 10; i++)
        {
            String topic = "Java_Topic";
            String value = "Matrix Number: " + Integer.toString(i);
            String key = "ID_" + Integer.toString(i);

            // Create Producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // Logging the Key
            // By Providing the Key, it is ensured that the same key goes to the same partition
            // Keys in different partition
            // Partition_0: ID_3, ID_4, ID_10
            // Partition_1: ID_1, ID_6, ID_8
            // Partition_2: ID_2, ID_5, ID_7, ID_9

            // Creating Callback >> Executes when the record is sent successfully or exception is thrown
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null)  // Record was sent successfully
                    {
                        logger.info("Received New MetaData. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else    // Exception occurred while sending the record
                        logger.error("Error while producing record!", e);
                }
            };

            // Send Data  --> Async Sending of Data
            producer.send(record, callback).get(); // Done to make the send-operation Synchronous. DO NOT DO IN PROD
        }
        // Flush Data
        producer.flush();

        // Flush and Close Producer Data
        producer.close();

    }
}
