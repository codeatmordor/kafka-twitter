package com.lm.gk.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello!");

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            String topic = "first_topic";
            String value = "hello world"+ Integer.toString(i);
            String key = "id_"+Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value,key);
            // send

            logger.info("Key : "+key);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes each successful/exception
                    if (e == null) {
                        logger.info("Received new Matadata \n" + "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Ts: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();// make this synchronous

        }
        producer.flush();
        producer.close();
        System.out.println("exit!");

    }
}
