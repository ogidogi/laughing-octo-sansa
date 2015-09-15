package com.test.spark.examples;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaSparkConsoleOutputExample {
    public static void main(String[] args) {
        String brokers = "quickstart.cloudera:9092";
        String topics = "test_topic";
        String fromOffset = "smallest";  //"smallest"; "largest";

        // Create context with 2 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

        // Get the lines
        messages
                .transformToPair(
                        rdd -> {
                            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                            offsetRanges.set(offsets);
                            return rdd;
                        })
                .foreachRDD(
                        rdd -> {
                            for (OffsetRange o : offsetRanges.get()) {
                                System.out.println(
                                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                                );
                                rdd.foreach(a -> {
                                    System.out.println(a);
                                });
                            }
                            return null;
                        }
                );
        // lines.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }
}
