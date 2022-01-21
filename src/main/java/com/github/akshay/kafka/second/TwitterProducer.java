package com.github.akshay.kafka.second;

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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer
{
    public TwitterProducer()
    {

    }
    public static void main(String[] args)
    {
        System.out.println("Hello from Twitter Producer!");

        new TwitterProducer().run();
    }

    public void run()
    {
        // Create a Twitter Client

        // Create a Kafka Producer

        // Loops to send tweets to Kafka
    }
    public void createTwitterClient()
    {
        String consumerKey = "747v46vZaSuE9H8YIZRltrP5s";
        String consumerSecret = "uIeCTkZlOhYPSSOYz2FTfoWRejEvLPx57LvlMZ8VIHbWWt7TjM";
        String token = "1483875169148243973-P9X5W7v5smyGM50bjJyJVqkuB2mB17";
        String secret = "lT6e5TldO8RiyKtHoV96HTmHfxmVuSSK54EVRm5c4BAJG";

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                         // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue)); // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
                hosebirdClient.connect();



    }

}
