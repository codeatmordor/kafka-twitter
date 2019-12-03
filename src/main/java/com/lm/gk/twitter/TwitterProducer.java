package com.lm.gk.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    public static void main(String[] args) {



    }

    public TwitterProducer(){}

    public void run(){}

    public void createTwitterClient(){

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "secret");

    }




}
