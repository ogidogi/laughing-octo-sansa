package org.bidsup.engine.consumers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage: DirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or more Kafka brokers <topics> is a list of one or
 * more kafka topics to consume from
 */
public final class TestSparkKafkaDirectApproach {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: DirectKafkaWordCount <brokers> <topics>\n" + " <brokers> is a list of one or more Kafka brokers\n"
                    + " <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];
        // Create context with 2 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(tuple2 -> tuple2._2());
        JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(SPACE.split(x)));
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey(
                (i1, i2) -> i1 + i2);
        wordCounts.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}