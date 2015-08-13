package com.test.spark.examples;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * <p/>
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <zkQuorum> is a list of one or more zookeeper servers that make quorum <group> is the name of kafka consumer group <topics> is a
 * list of one or more kafka topics to consume from <numThreads> is the number of threads the kafka consumer should use
 */
public final class TestSparkKafkaReceiverApproach {
    private static final Pattern SPACE = Pattern.compile(" ");

    private TestSparkKafkaReceiverApproach() {
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
                topicMap);
        JavaDStream<String> lines = messages.map(tuple2 -> tuple2._2());
        JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(SPACE.split(x)));
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey(
                (i1, i2) -> i1 + i2);
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}