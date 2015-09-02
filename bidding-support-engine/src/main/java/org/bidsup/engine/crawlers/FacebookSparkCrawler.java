package org.bidsup.engine.crawlers;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bidsup.engine.utils.FacebookUtils;

import com.restfb.Parameter;
import com.restfb.batch.BatchRequest;
import com.restfb.batch.BatchRequest.BatchRequestBuilder;

public class FacebookSparkCrawler {

    private static final Logger log = Logger.getLogger(FacebookSparkCrawler.class);

    public static void main(String[] args) throws ConfigurationException {
        FacebookSparkCrawler workflow = new FacebookSparkCrawler();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("facebook.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void run(CompositeConfiguration conf) {
        // Spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TwitterSparkCrawler").setMaster(conf.getString("spark.master"))
                .set("spark.serializer", conf.getString("spark.serializer"))
                .registerKryoClasses(new Class<?>[] { Parameter.class, BatchRequestBuilder.class, BatchRequest.class });
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(conf.getLong("stream.duration")));

        // Create facebook stream
        Parameter typeParam = Parameter.with("type", "event");
        FacebookUtils
                .createStream(jssc, conf.getString("access.token"),
                        new BatchRequestBuilder[] {
                                new BatchRequestBuilder("search").parameters(new Parameter[] { Parameter.with("q", "car"), typeParam }) })
                .print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
