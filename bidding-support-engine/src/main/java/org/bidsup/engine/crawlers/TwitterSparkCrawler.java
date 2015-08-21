package org.bidsup.engine.crawlers;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.bidsup.engine.beans.Tweet;

import com.fasterxml.jackson.databind.ObjectMapper;

import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class TwitterSparkCrawler {

    private static final Logger log = Logger.getLogger(TwitterSparkCrawler.class);

    public static void main(String[] args) throws ConfigurationException {
        TwitterSparkCrawler workflow = new TwitterSparkCrawler();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void run(CompositeConfiguration conf) {
        // Spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TwitterSparkCrawler").setMaster(conf.getString("spark.master"));
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(conf.getLong("stream.duration")));

        // Jackson mapper
        ObjectMapper mapper = new ObjectMapper();

        // Twitter4J
        // IMPORTANT: put keys in twitter4J.properties
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        // Create twitter stream
        String[] filters = { "#Car" };
        TwitterUtils.createStream(jssc, twitterAuth, filters).map(s -> new Tweet(s.getUser().getName(), s.getText(), s.getCreatedAt()))
                .map(t -> mapper.writeValueAsString(t)).foreachRDD(rdd -> {
                    rdd.foreach(a -> System.out.println(a));
                    return null;
                });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
