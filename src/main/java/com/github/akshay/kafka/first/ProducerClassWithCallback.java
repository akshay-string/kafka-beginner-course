package com.github.akshay.kafka.first;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClassWithCallback
{
    public static void main(String[] args)
    {
        Logger logger = LoggerFactory.getLogger(ProducerClassWithCallback.class);
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

            // Create Producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("Java_Topic", "Message Number: " + Integer.toString(i));

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
            producer.send(record, callback);
        }
        // Flush Data
        producer.flush();

        // Flush and Close Producer Data
        producer.close();



    }
}
