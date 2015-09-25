package org.bidsup.engine.consumers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.examples.h2o.CraigslistJobTitlesApp;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import hex.Model;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class StreamingUserTypeClassification {
    private static final Logger log = Logger.getLogger(StreamingUserTypeClassification.class);
    private static final String craigslistJobTitles = "/mnt/data/workspace/laughing-octo-sansa/data/craigslistJobTitles.csv";

    public static void main(String[] args) throws ConfigurationException {
        StreamingUserTypeClassification workflow = new StreamingUserTypeClassification();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("es.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void run(CompositeConfiguration conf) {
        // Kafka props
        String kafkaBrokers = conf.getString("metadata.broker.list");
        String topics = conf.getString("consumer.topic");
        String fromOffset = conf.getString("auto.offset.reset");

        // Spark props
        String sparkMaster = conf.getString("spark.master");
        String sparkSerDe = conf.getString("spark.serializer");
        long sparkStreamDuration = conf.getLong("stream.duration");

        SparkConf sparkConf = new SparkConf().setAppName("Kafka Spark ES Flow with Java API").setMaster(sparkMaster).set("spark.serializer",
                sparkSerDe);

        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sp, Durations.seconds(sparkStreamDuration));
        SQLContext sqlContext = new SQLContext(sp);
        H2OContext h2oContext = new H2OContext(sp.sc());
        h2oContext.start();

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        CraigslistJobTitlesApp staticApp = new CraigslistJobTitlesApp(craigslistJobTitles, sp.sc(), sqlContext, h2oContext);
        try {
            final Tuple2<Model<?, ?, ?>, Word2VecModel> tModel = staticApp.buildModels(craigslistJobTitles, "initialModel");
            final Word2VecModel w2vModel = tModel._2();
            final String modelId = tModel._1()._key.toString();

            // Create direct kafka stream with brokers and topics
            JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                    StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

            // Classify incoming messages
            messages.map(mesage -> mesage._2()).filter(str -> !str.isEmpty()).map(jobTitle -> staticApp.classify(jobTitle, modelId, w2vModel))
                    .map(pred -> new StringBuilder(100).append('\"').append(pred._1()).append("\" = ").append(Arrays.toString(pred._2())))
                    .print();

            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jssc.stop();
            staticApp.shutdown();
        }
    }
}
