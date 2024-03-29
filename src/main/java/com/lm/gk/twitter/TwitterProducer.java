    package com.lm.gk.twitter;

    import com.google.common.collect.Lists;
    import com.twitter.hbc.ClientBuilder;
    import com.twitter.hbc.core.Client;
    import com.twitter.hbc.core.Constants;
    import com.twitter.hbc.core.Hosts;
    import com.twitter.hbc.core.HttpHosts;
    import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
    import com.twitter.hbc.core.processor.StringDelimitedProcessor;
    import com.twitter.hbc.httpclient.auth.Authentication;
    import com.twitter.hbc.httpclient.auth.OAuth1;
    import java.util.Arrays;
    import org.apache.kafka.clients.producer.Callback;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerConfig;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.RecordMetadata;
    import org.apache.kafka.common.serialization.StringSerializer;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.util.List;
    import java.util.Properties;
    import java.util.concurrent.BlockingQueue;
    import java.util.concurrent.LinkedBlockingQueue;
    import java.util.concurrent.TimeUnit;

    public class TwitterProducer {

        public static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        private String consumerKey = "Qvx2bNKCl5Rjgpefvyx1pnnF9";
        private String consumerSecret = "pw0e7KGub7DtVfYOdpTVwRwHfJ6HUnDlvMu9HaUwHmonscmhCl";
        private String token = "3029879643-qubhsbTFe88OLkwnXio65L5fxYE1ET3Z5JvP7r6";
        private String secret = "dnPnRHzRGnwREf4CnfcMcv0erUo8CQIKmjMO9Z23RQbJV";

        List<String> terms = Lists.newArrayList("delhi","fire");



        public static void main(String[] args) {
            TwitterProducer tp = new TwitterProducer();
            tp.run();
        }

        public TwitterProducer() {
        }

        public void run() {

            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
            Client client = createTwitterClient(msgQueue,terms);
            client.connect();
            KafkaProducer<String,String> producer = createKafkaProducer();

            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                logger.info("Stopping Twitter Client");
                client.stop();
                logger.info("Closing producer.");
                producer.close();
                logger.info("Done!!");
            }));

            while (!client.isDone()) {
                String msg = null;
                try {
                    msg = msgQueue.poll(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    client.stop();
                }
                if (msg != null) {
                    logger.info("Received message : " + msg);
                    producer.send(new ProducerRecord<>("twitter_tweets", null, msg),
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if(e!=null){
                                    logger.error("Something wrong: ", e);
                                }
                            }
                        });
                }
            }
        }
        public Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
            List<Long> followings = Lists.newArrayList(1234L, 566788L);
            //List<String> terms = Lists.newArrayList("delhi", "fire");
            hosebirdEndpoint.followings(followings);
            hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            Client hosebirdClient = builder.build();
            return hosebirdClient;
        }

        public KafkaProducer<String,String> createKafkaProducer(){
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // create Safe Producer
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

            // High throughput producer
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

            KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

            return producer;
           // ProducerRecord<String,String> record = new ProducerRecord<>("first_topic", "hello deepak");


        }
    }
