package com.lm.gk.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-third-application";
        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerThread(bootStrapServer, groupId, latch);

        Thread cThread = new Thread(myConsumerRunnable);
        cThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook!!");
            ((ConsumerThread) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        }

    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


        public ConsumerThread(String bootstrapServer, String groupId, CountDownLatch latch) {

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton("first_topic"));
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value : " + record.value());
                    }

                }
            } catch (WakeupException e) {
                logger.error("Received Shutdown Signal!!");
            } finally {
                consumer.close();
                // tell main , we are done.
                latch.countDown();
            }

        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
