package com.gk.kafka;

import com.google.gson.JsonParser;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class StreamFilterTweets {

    public static void main(String[] args) {
        // create properties
        Properties props = new Properties();

        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String,String> inputTopic = sb.stream("twitter_topics");
        KStream<String,String> filteredStream = inputTopic.filter((k,jsonTweet)-> {
                JsonParser jsonParser = new JsonParser();
                try {
                    int followerCount = jsonParser.parse(jsonTweet).getAsJsonObject().get("user")
                        .getAsJsonObject().get("followers_count").getAsInt();
                    if (followerCount > 10000) {
                        return true;
                    }
                } catch (NullPointerException e) {
                    return false;
                }
                return false;
            }
        );

        filteredStream.to("important_tweets");

        // build topology

        KafkaStreams kafkaStreams = new KafkaStreams(sb.build(), props);


        // starts our stream application
        kafkaStreams.start();
    }

}
